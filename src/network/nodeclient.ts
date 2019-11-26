/**
 * @license
 * Copyright Coinversable B.V. All Rights Reserved.
 *
 * Use of this source code is governed by a AGPLv3-style license that can be
 * found in the LICENSE file at https://validana.io/license
 */

import * as net from "net";
import * as http from "http";
import * as https from "https";
import * as encryption from "crypto";
import { Worker } from "cluster";
import { QueryConfig } from "pg";
import { Block, Log, Crypto, PublicKey } from "@coinversable/validana-core";
import { Config } from "../config";
import { Client, NodeRequest, ProcessorResponse } from "./client";
import { Peer, RetrievingBlocks } from "./peer";

/** Information about blocks the client has downloaded or is downloading. */
interface DownloadBlocks {
	start: number;
	amount: number;
	data: Block[] | Readonly<Peer>;
}

/**
 * The node client is the network client for nodes.
 * It will retrieve blocks and peers from other nodes and provide them as well.
 */
export class NodeClient extends Client {
	protected lastRetrievedProcessorInfo: number | undefined;

	/** All block ranges that it has currently downloaded and is currently downloading. */
	protected downloadBlocks: DownloadBlocks[] = [];
	protected isStoringBlocks: boolean = false;

	/** Create a new client for a node. */
	constructor(worker: Worker, config: Readonly<Config>) {
		super(worker, config);
		this.processorPubKey = new PublicKey(Crypto.hexToBinary(this.config.VNODE_PUBLICKEY));

		//Retrieve how far we are with downloading the blockchain from the database.
		this.getLatestRetrievedBlock().then((blockId) => {
			this.latestRetrievedBlock = blockId;
			this.createServer();
		});

		//If everything is up and running report back memory usage, otherwise we try again later...
		setInterval(() => {
			if (this.server !== undefined && this.server.listening && !this.isClosingServer) {
				this.worker.send({ type: "report", memory: process.memoryUsage().heapTotal / 1024 / 1024 });
			}
		}, 20000);
	}

	/**
	 * Called when a peer has new information for us.
	 * @param o The peer that has new information.
	 * @param arg What new information it has.
	 */
	public update(o: Peer, arg?: RetrievingBlocks | Array<{ ip: string, port: number }> | number): void {
		if (arg === undefined) {
			//A peer disconnected
			this.peers.splice(this.peers.indexOf(o), 1);
			this.badPeers.push(o);
			setTimeout(() => this.badPeers.slice(this.badPeers.indexOf(o), 1), this.config.VNODE_PEERTIMEOUT);

			//Get new peers now that we have one less
			this.getPeers();

			//Remove all outstanding downloads from this peer.
			for (let i = this.downloadBlocks.length - 1; i >= 0; i--) {
				if (this.downloadBlocks[i].data === o) {
					this.downloadBlocks.splice(i, 1);
				}
			}
			//Retrieve new blocks (that may have been requested by this peer but no longer are)
			this.storeAndGetBlocks();

		} else if (typeof arg === "number") {
			//Received a new highest block, check if we still need this block and aren't already requesting blocks from this peer.
			if (arg > this.latestRetrievedBlock && o.retrievingBlocks === undefined) {
				this.storeAndGetBlocks();
			}
		} else if (arg instanceof Array) {
			//Received new peers to connect to
			for (const newPeerInfo of arg) {
				//Check if we have enough peers (allow a few more peers then needed so we don't redo this every disconnect)
				if (this.peers.length > (this.config.VNODE_MINPEERS + this.config.VNODE_MAXPEERS) / 2) {
					break;
				}
				if (!this.hasPeer(newPeerInfo.ip, newPeerInfo.port)) {
					this.peers.push(new Peer(this, { ip: newPeerInfo.ip, port: newPeerInfo.port }));
				}
			}
			//Get even more peers if needed
			this.getPeers();
		} else {
			//BlockRequest has been (partially) fulfilled
			const downloadBlocksGroups = [];
			let downloadBlocks: DownloadBlocks = {
				start: arg.start,
				amount: 0,
				data: []
			};

			//Split it into multiple DownloadBlocks with consecutive blocks
			for (let i = 0; i < arg.amount; i++) {
				if (arg.blocks[i] !== undefined) {
					(downloadBlocks.data as Block[]).push(arg.blocks[i]);
					downloadBlocks.amount++;
				} else {
					if (downloadBlocks.amount > 0) {
						downloadBlocksGroups.push(downloadBlocks);
					}
					downloadBlocks = {
						start: arg.start + i + 1,
						amount: 0,
						data: []
					};
				}
			}
			if (downloadBlocks.amount > 0) {
				downloadBlocksGroups.push(downloadBlocks);
			}
			//Put them all into downloadBlocks and sort it.
			const filledRequest = this.downloadBlocks.findIndex((downloadBlock) => downloadBlock.data === o);
			if (filledRequest !== -1) {
				this.downloadBlocks.splice(filledRequest, 1);
			} else {
				//Not much we can do here, we have more data then we should have, which shouldn't happen but isn't really a problem.
				Log.error("Unknown request fulfilled.");
			}
			this.downloadBlocks.push(...downloadBlocksGroups);
			this.storeAndGetBlocks();
		}
	}

	/** Create a server for other peers to connect to. It will also retrieve info from the processor once it has started. */
	private createServer(): void {
		let timeout = 5000;

		this.server = net.createServer();
		this.server.maxConnections = this.config.VNODE_MAXPEERS;

		this.server.on("listening", () => {
			timeout = 5000;
			//In case no port was specified we set it to the port that was giving.
			this.port = (this.server!.address() as net.AddressInfo).port;

			//Request info from the processor.
			this.retrieveProcessorInfo(() => {
				//Start accepting incomming connections
				if (this.server !== undefined) {
					this.server.on("connection", (socket) => {
						const addr = socket.address() as net.AddressInfo;
						if (this.peers.length >= this.config.VNODE_MAXPEERS || this.hasPeer(addr.address, addr.port)) {
							socket.end();
						} else {
							const newPeer = new Peer(this, socket);
							this.peers.push(newPeer);
							if (this.peers.length < this.config.VNODE_MINPEERS) {
								newPeer.getPeers();
							}
						}
					});
				}

				//Get more peers if needed.
				this.getPeers();
			});
		});

		//Restart the server in a bit after an error.
		this.server.on("error", (error) => {
			Log.warn("Peer server error", error);
			if (!this.server!.listening) {
				timeout = Math.min(timeout * 1.5, 300000);
				//We got an error while starting up
				this.shutdownServer().then(() => setTimeout(() => {
					if (!this.permanentlyClosed) {
						this.server!.listen(this.config.VNODE_LISTENPORT);
					}
				}, timeout)).catch();
			} else {
				//We got an error while we are listening
				this.shutdownServer().then(() => setTimeout(() => {
					if (!this.permanentlyClosed) {
						this.server!.listen(this.config.VNODE_LISTENPORT);
					}
				}, timeout)).catch();
			}
		});

		this.server.listen(this.port);
	}

	/**
	 * Store blocks that we have downloaded (if we have any to store) and download new blocks.
	 * This will also update latest retrieved block and cached blocks.
	 */
	private async storeAndGetBlocks(): Promise<void> {
		//Sort all blocks that we have downloaded
		this.downloadBlocks.sort((a, b) => (a.start - b.start));

		//Store new blocks if we have any to store.
		if (!this.isStoringBlocks && this.downloadBlocks.length > 0 && this.latestRetrievedBlock + 1 === this.downloadBlocks[0].start
			&& this.downloadBlocks[0].data instanceof Array) {

			//Generate the start of the insert query.
			let isFirstBlock = true;
			let query = "INSERT INTO basics.blocks (block_id, version, previous_block_hash, processed_ts, transactions, transactions_amount, signature) VALUES ";
			const values = [];
			//For each blocks add another line to the insert query and add all values.
			for (const block of this.downloadBlocks[0].data as Block[]) {
				if (!isFirstBlock) {
					query += ", ";
				}
				isFirstBlock = false;
				query += (`($${values.length + 1}, $${values.length + 2}, $${values.length + 3}, $${values.length + 4}, $${values.length + 5}, $${values.length + 6}, $${values.length + 7})`);
				values.push(block.id, block.version, block.getPreviousBlockHash(), block.processedTs, block.getTransactions(), block.transactionsAmount, block.getSignature());
			}
			query += ";";

			//Insert the block
			this.isStoringBlocks = true;
			try {
				await this.query({ text: query, values });
				this.cachedBlocks.push(...this.downloadBlocks[0].data as Block[]);
				if (this.cachedBlocksStartId === -1) {
					this.cachedBlocksStartId = this.downloadBlocks[0].start;
				}
				if (this.cachedBlocks.length > this.config.VNODE_CACHEDBLOCKS) {
					this.cachedBlocksStartId = this.cachedBlocksStartId + this.cachedBlocks.length - this.config.VNODE_CACHEDBLOCKS;
					this.cachedBlocks.splice(0, this.cachedBlocks.length - this.config.VNODE_CACHEDBLOCKS);
				}
				this.latestRetrievedBlock = this.downloadBlocks[0].start + this.downloadBlocks[0].amount - 1;
				if (this.latestRetrievedBlock > this.latestExistingBlock) {
					this.latestExistingBlock = this.latestRetrievedBlock;
				}
				for (const peer of this.peers) {
					peer.newLatestKnown(this.latestRetrievedBlock);
				}
				this.downloadBlocks.splice(0, 1);
				this.isStoringBlocks = false;
				this.storeAndGetBlocks();
			} catch (error) {
				Log.warn("Failed to insert blocks in DB", error);
				if (error.message.indexOf("violates check constraint") !== -1 ||
					error.message.indexOf("violates unique constraint") !== -1 ||
					error.message.indexOf("violates not-null constraint") !== -1) {

					//This should only happen if the database is setup incorrectly or the processor signed an invalid message.
					return Log.fatal("Database setup incorrectly or processor signed invalid message.").then(error, () => process.exit(50));
				} else if (error.message.indexOf("Connection terminated") !== -1) {
					//The block may or may not have been succesfully stored, retry after verifying
					setTimeout(() => this.getLatestRetrievedBlock().then((blockId) => {
						if (this.downloadBlocks[0].start < blockId) {
							this.downloadBlocks.splice(0, 1);
						}
						this.latestRetrievedBlock = blockId;
						this.isStoringBlocks = false;
						this.storeAndGetBlocks();
					}), 1000);
				} else {
					//Failed for another reason, retry later
					this.isStoringBlocks = false;
					setTimeout(() => this.storeAndGetBlocks(), 1000);
				}
			}
		}

		//List all blocks that we still need to download.
		const toDownload: Array<{ start: number, amount: number }> = [];
		let start = this.latestRetrievedBlock + 1;
		for (const block of this.downloadBlocks) {
			if (block.start > start) {
				toDownload.push({ start, amount: block.start - start });
			}
			start = block.start + block.amount;
		}
		if (start < this.latestRetrievedBlock + 1 + this.config.VNODE_MAXOUTSTANDINGLBOCKS) {
			toDownload.push({ start, amount: this.latestRetrievedBlock + 1 + this.config.VNODE_MAXOUTSTANDINGLBOCKS - start });
		}

		//Sort peers starting with lowest latestKnownBlock
		this.peers.sort((a, b) => (a.latestKnownBlock - b.latestKnownBlock));
		//Find peers who can provide the blocks that still need to be downloaded.
		for (const block of toDownload) {
			for (const peer of this.peers) {
				if (peer.retrievingBlocks === undefined && block.start <= peer.latestKnownBlock) {
					const requestAmount = Math.min(block.amount, peer.latestKnownBlock - block.start + 1, this.config.VNODE_MAXREQUESTAMOUNT);
					peer.requestBlocks(block.start, requestAmount);
					this.downloadBlocks.push({ start: block.start, amount: requestAmount, data: peer });

					//Calculate the amount still needed for this block.
					block.start += requestAmount;
					block.amount -= requestAmount;
					//If this block is done skip giving it to other peers and go to the next block.
					if (block.amount <= 0) {
						break;
					}
				}
			}
			if (block.amount <= 0) {
				continue;
			}
		}
	}

	/**
	 * Get new peers if we go below the maximum amount of peers.
	 * @param noNewBlocks If it is searching for new peers because no new blocks are comming in.
	 */
	private getPeers(noNewBlocks: boolean = false): void {
		if (this.peers.length < this.config.VNODE_MINPEERS || noNewBlocks) {
			let isRetrievingPeers = false;
			const now = Date.now();
			//If other peers their peers do still receive blocks we should be receiving them as well, so no point doing this
			if (!noNewBlocks) {
				//Randomize which peer we ask for new peers.
				const start = Math.floor(Math.random() * this.peers.length);
				for (let i = start; i < start + this.peers.length; i++) {
					const peer = this.peers[i % this.peers.length];
					//Get peers from other peers at most once every 5 minutes
					if (!peer.isRetrievingPeers && (peer.lastRetrievedPeers === undefined || peer.lastRetrievedPeers + 300 * 1000 < now)) {
						peer.getPeers();
						isRetrievingPeers = true;
						break;
					}
				}
			}

			//Retrieve peers from the processor at most once every 5 minutes
			if (!isRetrievingPeers && (this.lastRetrievedProcessorInfo === undefined || this.lastRetrievedProcessorInfo + 300 * 1000 < now)) {
				this.retrieveProcessorInfo();
			}
		}
	}

	/**
	 * Retrieve information from the processor about the blockchain as well as the first set of peers.
	 * @param cb The callback to call once finished
	 * @param timeout The timeout after which to try again should it fail
	 */
	private retrieveProcessorInfo(cb?: () => void, timeout = 5000): void {
		this.lastRetrievedProcessorInfo = Date.now();

		const requestCallback = (response: http.IncomingMessage) => {
			let data: Buffer = Buffer.alloc(0);
			response.on("data", (chunk) => {
				if (typeof chunk === "string") {
					data = Buffer.concat([data, Crypto.utf8ToBinary(chunk)]);
				} else {
					data = Buffer.concat([data, chunk]);
				}
			});
			response.on("error", (error) => {
				Log.warn("Problem with response", error);
				setTimeout(() => this.retrieveProcessorInfo(cb, Math.min(timeout * 1.5, 300000)), timeout);
			});

			response.on("end", () => {
				if (response.statusCode === 200) {
					let body: ProcessorResponse;
					//Https will authenticate the processor and ensure data cannot be modified, but everyone can connect
					//to the processor and we want only those with the key to be able to understand the response.
					try {
						if (this.encryptionKey !== undefined) {
							const toDecrypt = data.slice(16);
							const decryptionCipher = encryption.createDecipheriv("AES-256-CTR", this.encryptionKey, data.slice(0, 16));
							body = JSON.parse(Crypto.binaryToUtf8(decryptionCipher.update(toDecrypt)));
						} else {
							body = JSON.parse(Crypto.binaryToUtf8(data));
						}
					} catch (error) {
						//We won't be able to continue anymore.
						Log.fatal("Unable to decode response from the processor, make sure you use the right encryption key!",
							undefined).then(() => process.exit(51));
						return;
					}
					const queryConfig: QueryConfig = {
						text: "INSERT INTO basics.info (key, value) VALUES ('blockInterval', $1), ('processorNodeVersion', $2), ('signPrefix', $3), " +
							"('processorPostgresVersion', $4) ON CONFLICT ON CONSTRAINT info_pkey DO UPDATE SET value = $1 WHERE info.key = 'blockInterval';",
						values: [body.blockInterval.toString(), body.nodeVersion, body.signPrefix, body.postgresVersion.toString()]
					};
					this.query(queryConfig).catch((error) => {
						Log.warn("Failed to store retrieved processor information.", error);
						setTimeout(() => this.retrieveProcessorInfo(cb, Math.min(timeout * 1.5, 300000)), timeout);
					}).then(() => {
						this.ip = body.externalIp;
						this.lastRetrievedProcessorInfo = Date.now();
						this.signPrefix = Crypto.utf8ToBinary(body.signPrefix);
						this.latestExistingBlock = body.latestBlock;
						this.blockMaxSize = body.blockMaxSize;

						//Set a timeout that increases the latestExistingBlock
						if (this.latestExistingBlockInterval !== undefined) {
							clearInterval(this.latestExistingBlockInterval);
						}
						this.latestExistingBlockInterval = setInterval(() => {
							this.latestExistingBlock++;
							//If we are more than 10 blocks behind (and not downloading blocks right now):
							if (this.downloadBlocks.length === 0 && this.latestRetrievedBlock < this.latestExistingBlock - 10) {
								this.getPeers(true);
							}
						}, body.blockInterval * 1000);

						//Create and connect to peers, dropping random old peers if needed.
						const connectedPeers: Array<Readonly<Peer>> = [];
						for (const peer of this.peers) {
							connectedPeers.push(peer);
						}
						for (const peer of body.peers) {
							if (!this.hasPeer(peer.ip, peer.port)) {
								if (this.peers.length >= this.config.VNODE_MAXPEERS && connectedPeers.length > 0) {
									//Too many peers, but we have some that we can drop
									const randomPeer = Math.floor(Math.random() * connectedPeers.length);
									connectedPeers[randomPeer].disconnect();
									connectedPeers.splice(randomPeer, 1);
								} else if (this.peers.length < this.config.VNODE_MAXPEERS) {
									//Not too many peers
									this.peers.push(new Peer(this, { ip: peer.ip, port: peer.port }));
								}
							}
						}
						if (cb !== undefined) {
							cb();
						}
					});
				} else {
					Log.warn(`Failed to retrieve processor info, result code: ${response.statusCode}`);
					setTimeout(() => this.retrieveProcessorInfo(cb, Math.min(timeout * 1.5, 300000)), timeout);
				}
			});
		};

		//If we have to retrieve the info from the processor node
		const requestData = Crypto.utf8ToBinary(JSON.stringify({
			port: this.port,
			version: Client.version,
			oldestCompVersion: Client.oldestCompatibleVersion
		} as NodeRequest));

		const requestOptions = {
			host: this.config.VNODE_PROCESSORHOST,
			port: this.config.VNODE_PROCESSORPORT,
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				"content-Length": requestData.length
			},
			rejectUnauthorized: true //Is not the default in older versions
		};

		const request = this.config.VNODE_TLS ? https.request(requestOptions, requestCallback) : http.request(requestOptions, requestCallback);
		request.on("error", (error) => {
			Log.warn(`Error with processor connection`, error);
			setTimeout(() => this.retrieveProcessorInfo(cb, Math.min(timeout * 1.5, 300000)), timeout);
		});
		request.write(requestData);
		request.end();
	}
}