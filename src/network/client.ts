/**
 * @license
 * Copyright Coinversable B.V. All Rights Reserved.
 *
 * Use of this source code is governed by a AGPLv3-style license that can be
 * found in the LICENSE file at https://validana.io/license
 */

import * as net from "net";
import { Worker } from "cluster";
import { Client as DBClient, QueryResult, QueryConfig, types } from "pg";
import { Crypto, Log, Block, DBBlock, PublicKey } from "@coinversable/validana-core";
import { VObserver } from "../tools/observer";
import { Config } from "../config";
import { Peer, RetrievingBlocks } from "./peer";

/** Make sure if we query the database any BIGINTs are returned as a number, instead of a string. */
types.setTypeParser(20, (val: string) => {
	return Number.parseInt(val, 10);
});

/** Request from the node for information about the blockchain. */
export interface NodeRequest {
	port: number;
	version: number;
	oldestCompVersion: number;
}

/** Response from the processor with information about the blockchain. */
export interface ProcessorResponse {
	nodeVersion: string;
	postgresVersion: number;
	signPrefix: string;
	blockInterval: number;
	blockMaxSize: number;
	latestBlock: number;
	externalIp: string;
	peers: Array<{
		ip: string;
		port: number;
	}>;
}

/**
 * The client contains the basic functions that both the processor client and the node client require.
 * This includes sharing its own blocks and peers with others.
 */
export abstract class Client implements VObserver<RetrievingBlocks | Array<{ ip: string, port: number }> | number | boolean> {
	/** Our version, oldest compatible version and extension flags that we support. Version 0 and 255 are reserved for testing. */
	public static readonly version = 1;
	public static readonly oldestCompatibleVersion = 1;
	public static readonly extensionFlags = Crypto.hexToBinary("00000000");

	public readonly config: Readonly<Config>;
	public readonly worker: Worker;
	public readonly encryptionKey: Buffer | undefined; //undefined if we don't use encryption
	protected server: net.Server | undefined; //undefined till it is created
	protected isClosingServer: boolean = false; //Is it currently closing the server?
	protected permanentlyClosed: boolean = false; //Is this client currently shutting down?
	public port: number | undefined; //Port we are listening on for incomming connections (or undefined if we don't know yet)
	public ip: string | "processor" | undefined; //Our ip address (or "processor" if it is the processor or undefined if we don't know yet)

	protected client: DBClient | undefined;
	public latestRetrievedBlock: number = -1; //Latest block we have retrieved

	protected latestExistingBlock: number = 0; //Latest block that exists in the blockchain
	protected latestExistingBlockInterval: NodeJS.Timer | undefined; //Timer that increases the amount of blocks that exist in the blockchain
	public signPrefix: Buffer = Buffer.alloc(0);
	public prefixLength: Buffer = Crypto.uInt8ToBinary(0);
	public processorPubKey: PublicKey | undefined; //Undefined for the processor, as it doesn't need to verify blocks
	public blockMaxSize: number = 110000; //The max size a block may be (at least 1 max size transaction + some extra for overhead)
	public peers: Array<Readonly<Peer>> = []; //The peers we are connected to
	protected badPeers: Array<Readonly<Peer>> = []; //The peers we recently disconnected from for some reason

	protected cachedBlocks: Block[] = []; //Blocks cached in memory so it doesn't have to retrieve recent blocks from the DB all the time
	protected cachedBlocksStartId: number = -1; //Id of first block cached in memory

	/**
	 * Create a new client.
	 * @param worker The worker that created this client.
	 * @param config The config to use
	 */
	constructor(worker: Worker, config: Readonly<Config>) {
		this.worker = worker;
		this.config = config;
		if (config.VNODE_ENCRYPTIONKEY !== "") {
			this.encryptionKey = Crypto.hexToBinary(config.VNODE_ENCRYPTIONKEY);
		}
		if (config.VNODE_LISTENPORT !== 0) {
			this.port = this.config.VNODE_LISTENPORT;
		}
	}

	/**
	 * Called when a peer has new information for us.
	 * @param o The peer that has new information.
	 * @param arg What new information it has.
	 */
	public abstract update(o: Peer, arg?: RetrievingBlocks | Array<{ ip: string, port: number }> | number): void;

	/**
	 * Get the id of the latest block that is currently in the database or -1 if there are none. It will retry until it succeeds.
	 * @param timeout The timeout after which to try again should it fail
	 */
	protected async getLatestRetrievedBlock(timeout = 5000): Promise<number> {
		try {
			const result = await this.query({ text: "SELECT * FROM basics.blocks ORDER BY block_id DESC LIMIT 1;" });
			if (result.rows.length === 0) {
				return -1;
			} else {
				return result.rows[0].block_id;
			}
		} catch (error) {
			Log.warn("Failed to retrieve database info", error);
			return new Promise<number>((resolve) => setTimeout(() => resolve(this.getLatestRetrievedBlock(Math.min(timeout * 1.5, 300000))), timeout));
		}
	}

	/**
	 * Query the database. Will connect to the database if it is not currently connected.
	 * @param queryConfig The query to execute
	 */
	protected async query(queryConfig: QueryConfig): Promise<QueryResult> {
		if (this.client === undefined) {
			this.client = new DBClient({
				user: this.config.VNODE_DBUSER,
				database: this.config.VNODE_DBNAME,
				password: this.config.VNODE_DBPASSWORD,
				port: this.config.VNODE_DBPORT,
				host: this.config.VNODE_DBHOST
			}).on("error", (error) => {
				this.client = undefined;
				//Do not accidentally capture password
				error.message = error.message.replace(new RegExp(this.config.VNODE_DBPASSWORD!, "g"), "");
				Log.warn("Problem with database connection.", error);
			}).on("end", () => this.client = undefined);
			await this.client.connect();
		}
		return this.client.query(queryConfig);
	}

	/**
	 * Let other peers request blocks from this client.
	 * @param start The first block to request
	 * @param amount The amount of blocks to request
	 */
	public async getBlocks(start: number, amount: number): Promise<Block[]> {
		const result: Block[] = [];
		let end = start + amount;
		if (start >= this.cachedBlocksStartId && start < this.cachedBlocksStartId + this.cachedBlocks.length) {
			//We have everything cached, return immideately.
			return this.cachedBlocks.slice(start - this.cachedBlocksStartId, end - this.cachedBlocksStartId);
		}
		if (end >= this.cachedBlocksStartId && end < this.cachedBlocksStartId + this.cachedBlocks.length) {
			//We have part of it cached, add that to the result and add the rest from the DB.
			result.push(...this.cachedBlocks.slice(0, end - this.cachedBlocksStartId));
			//This is the new end of what we still need to retrieve.
			end = this.cachedBlocksStartId;
		}
		try {
			const blocksResult = await this.query({
				text: "SELECT * FROM basics.blocks WHERE block_id >= $1 AND block_id < $2 ORDER BY block_id DESC;",
				values: [start, end]
			});
			for (const blockResult of blocksResult.rows as DBBlock[]) {
				//Block in transit (and cache) exists of total length, version, blockId, hash, processedTs, transactions and signature
				result.unshift(new Block(blockResult));
			}
		} catch (error) {
			//Failed to retrieve (more) blocks from the DB
			Log.warn("Failed to retrieve blocks from the DB.", error);
		}
		//Send back the blocks that we do have if any.
		return result;
	}

	/** Get whether or not we already have this peer currently or is in the bad peer list. */
	protected hasPeer(ip: string, port: number, onlyConnected: boolean = false): boolean {
		for (const peer of this.peers) {
			if (ip === peer.ip && (port === peer.connectionPort || port === peer.listenPort)) {
				return true;
			}
		}
		if (!onlyConnected) {
			for (const peer of this.badPeers) {
				if (ip === peer.ip && (port === peer.connectionPort || port === peer.listenPort)) {
					return true;
				}
			}
		}
		return false;
	}

	/** Shuts down the server. */
	public async shutdownServer(): Promise<void> {
		if (this.isClosingServer) {
			if (this.permanentlyClosed) {
				return Promise.resolve();
			} else {
				return Promise.reject(new Error("Server already closing."));
			}
		} else {
			this.isClosingServer = true;
			return new Promise<void>((resolve) => {
				this.server!.close(() => {
					this.isClosingServer = false;
					resolve();
				});
			});
		}
	}

	/** Shutdown the process. An error code between 50 and 59 means it should stay down due to an error it cannot recover from. */
	public async shutdown(exitCode: number = 0): Promise<never> {
		this.permanentlyClosed = true;
		const promises: Array<Promise<void>> = [];

		//Disconnect from all peers, the server will wait till there are no more open connections before calling the close callback.
		for (const peer of this.peers) {
			promises.push(peer.disconnect(undefined));
		}

		//We may already be closing the servers, but it will not be restarted twice anyway considering we are shutting down.
		if (this.server !== undefined && this.server.listening) {
			this.isClosingServer = true;
			promises.push(this.shutdownServer());
		}

		//Do not add promises that may reject
		await Promise.all(promises);

		if (this.client !== undefined) {
			try {
				await this.client.end();
			} catch (error) {
				Log.warn("Failed to properly shutdown database client.", error);
				if (exitCode === 0) {
					exitCode = 1;
				}
			}
		}

		return process.exit(exitCode);
	}
}