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
import * as FS from "fs";
import * as encryption from "crypto";
import { Worker } from "cluster";
import { Log, DBBlock, Block, Crypto } from "@coinversable/validana-core";
import { Config } from "../config";
import { Client, NodeRequest, ProcessorResponse } from "./client";
import { Peer, RetrievingBlocks } from "./peer";

/**
 * The processor client is the network client for the processor.
 * Unlike the other nodes it does not request new block or peers, only provide them.
 * In addition it is the starting point for other peers to find the first peers.
 */
export class ProcessorClient extends Client {
	protected postgresVersion: number = 0;
	protected infoServer: http.Server | https.Server | undefined;
	protected isClosingInfoServer: boolean = false;
	protected seenPeers: Array<{ ip: string, port: number, lastSeen: number, version: number, oldestCompVersion: number }> = [];
	protected watchingCert: boolean = false;
	protected isReloadingCert: boolean = false;

	/** Create a new client for the processor. */
	constructor(worker: Worker, config: Readonly<Config>) {
		super(worker, config);
		this.signPrefix = Crypto.utf8ToBinary(this.config.VNODE_SIGNPREFIX);
		this.latestExistingBlock = this.config.VNODE_LATESTEXISTINGBLOCK;
		this.ip = "processor";

		//Create the peer server (but do not yet start listening, this will create the info server once it starts listening)
		this.createPeerServer();

		//Retrieve the database version
		this.getDatabaseVersion().then((version) => {
			this.postgresVersion = version;
			//Retrieve the last known block from the database
			this.getLatestRetrievedBlock().then((blockId) => {
				this.latestRetrievedBlock = blockId;
				if (blockId > this.latestExistingBlock) {
					this.latestExistingBlock = blockId;
				}
				//Update with new blocks once in a while
				this.cacheBlocks();
				this.latestExistingBlockInterval = setInterval(() => this.cacheBlocks(), this.config.VNODE_BLOCKINTERVAL * 1000);

				//Start the peer server
				this.server!.listen(this.port);
			});
		});

		//If everything is up and running report back memory usage, otherwise we try again later...
		setInterval(() => {
			if (this.server !== undefined && this.server.listening && !this.isClosingServer &&
				this.infoServer !== undefined && this.infoServer.listening && !this.isClosingInfoServer) {
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
		//We only care about disconnects as the processor client
		if (arg === undefined) {
			//A peer disconnected
			this.peers.splice(this.peers.indexOf(o), 1);
			this.badPeers.push(o);
			setTimeout(() => this.badPeers.slice(this.badPeers.indexOf(o), 1), this.config.VNODE_PEERTIMEOUT);
		}
	}

	/**
	 * Get what database version is being used. It will retry until it succeeds.
	 * @param timeout The timeout after which to try again should it fail
	 */
	private async getDatabaseVersion(timeout = 5000): Promise<number> {
		try {
			const result = await this.query({ text: "SELECT current_setting('server_version_num');" });
			return result.rows[0].current_setting;
		} catch (error) {
			Log.warn("Failed to retrieve database version", error);
			return new Promise<number>((resolve) => setTimeout(() => resolve(this.getDatabaseVersion(Math.min(timeout * 1.5, 300000))), timeout));
		}
	}

	/** Will get new blocks from the database and cache them. */
	private async cacheBlocks(): Promise<void> {
		try {
			const result = await this.query({
				text: "SELECT * FROM basics.blocks WHERE block_id > $1 ORDER BY block_id ASC LIMIT $2;",
				values: [
					//Request only new ones or if we don't have any cached block yet request older ones as well
					this.cachedBlocks.length === 0 ? this.latestRetrievedBlock - this.config.VNODE_CACHEDBLOCKS : this.latestRetrievedBlock,
					this.config.VNODE_CACHEDBLOCKS
				],
				name: "cached"
			});
			const blocks: DBBlock[] = result.rows;
			if (blocks.length > 0) {
				//Cache the blocks
				for (const block of blocks) {
					if (this.cachedBlocks.length === 0 || this.cachedBlocks[this.cachedBlocks.length - 1].id + 1 === block.block_id) {
						this.cachedBlocks.push(new Block(block));
					} else {
						Log.error("Retrieved cached blocks with too old/new ids.");
						this.cachedBlocks = [];
						this.cachedBlocksStartId = -1;
					}
				}
				if (this.cachedBlocks.length > this.config.VNODE_CACHEDBLOCKS) {
					this.cachedBlocks.splice(0, this.cachedBlocks.length - this.config.VNODE_CACHEDBLOCKS);
				}
				this.cachedBlocksStartId = this.cachedBlocks[0].id;

				//Update latest known
				const lastBlockId = blocks[blocks.length - 1].block_id;
				if (lastBlockId > this.latestExistingBlock) {
					this.latestExistingBlock = lastBlockId;
				}
				if (lastBlockId > this.latestRetrievedBlock) {
					this.latestRetrievedBlock = lastBlockId;
					for (const peer of this.peers) {
						peer.newLatestKnown(this.latestRetrievedBlock);
					}
				}
			}
		} catch (error) {
			//It will be tried again because of the setInterval
			Log.warn("Failed to retrieve database info", error);
		}
	}

	/**
	 * Create a server for other peers to connect to, but do not yet start listening.
	 * Will also create the info server once it starts listening.
	 */
	private createPeerServer(): void {
		let timeout = 5000;

		//Setup a server for incomming connections.
		this.server = net.createServer();
		this.server.maxConnections = this.config.VNODE_MAXPEERS;

		this.server.on("listening", () => {
			timeout = 5000;
			//In case no port was specified we set it to the port that was giving.
			this.port = (this.server!.address() as net.AddressInfo).port;
			//Setup the info server if this is the first time
			if (this.infoServer === undefined) {
				this.createInfoServer();
			}
		});

		//If a new peer connects to us.
		this.server.on("connection", (socket) => {
			if (this.peers.length >= this.config.VNODE_MAXPEERS) {
				socket.end();
			} else {
				this.peers.push(new Peer(this, socket));
			}
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
	}

	/**
	 * Create a server for nodes to request information about the blockchain.
	 * Will start listening after it has been created.
	 */
	private createInfoServer(): void {
		let timeout = 5000;

		//Setup the rest server
		if (!this.config.VNODE_TLS) {
			this.infoServer = http.createServer();
		} else {
			this.infoServer = https.createServer(this.loadCertificate()!);
			if (!this.watchingCert) {
				this.watchingCert = true;
				//If the file changes give it a second for both the key and the cert to change, then reload.
				FS.watchFile(this.config.VNODE_CERTPATH, (curr, prev) => {
					//Check if the file was modified and not just opened.
					if (curr.mtime !== prev.mtime) {
						setTimeout(() => {
							Log.info("Reloading certificate.");
							const newCertificate = this.loadCertificate();
							//Only reload if it succeeded loading the files.
							if (newCertificate !== undefined) {
								try {
									//Reloading certificates is not officially supported, but it works anyway.
									(this.infoServer as any)._sharedCreds.context.setCert(newCertificate.cert);
									(this.infoServer as any)._sharedCreds.context.setKey(newCertificate.key);
								} catch (error) {
									//Do not log possible certificate
									Log.error("Problem with reloading certificate.");
								}
							}
						}, 5000);
					}
				});
			}
		}

		this.infoServer.on("listening", () => timeout = 5000);

		//What to do if there is an incomming connection
		this.infoServer.on("request", (req: http.IncomingMessage, res: http.ServerResponse) => {
			if (req.method === "POST") {
				let body: string = "";
				//Read part of the body
				req.on("data", (postData) => {
					body += postData.toString();
					if (body.length > 10000) {
						res.writeHead(413);
						res.end("Payload too large.");
						res.connection.destroy();
						return;
					}
				});

				//Finished reading body
				req.on("end", () => {
					let data: NodeRequest;
					try {
						data = JSON.parse(body);
					} catch (error) {
						res.writeHead(400);
						res.end("Invalid request json.");
						return;
					}
					//Check if all values are valid.
					if (!Number.isInteger(data.oldestCompVersion) || !Number.isInteger(data.version) || !Number.isInteger(data.port) ||
						data.oldestCompVersion < 0 || data.oldestCompVersion >= 256 || data.version < 0 || data.version >= 256 ||
						data.port <= 0 || data.port > 65535 || data.oldestCompVersion > data.version) {

						res.writeHead(400);
						res.end("Invalid request data.");
						return;
					}
					this.respond(res, req.socket.remoteAddress!, data);
				});
			} else {
				res.writeHead(405);
				res.end("Invalid request method.");
				return;
			}
		});

		//Restart the server in a bit after an error.
		this.infoServer.on("error", (error) => {
			Log.warn("Info server error", error);
			if (!this.infoServer!.listening) {
				timeout = Math.min(timeout * 1.5, 300000);
				//We got an error while starting up
				this.shutdownInfoServer().then(() => setTimeout(() => {
					if (!this.permanentlyClosed) {
						this.infoServer!.listen(this.config.VNODE_PROCESSORPORT);
					}
				}, timeout)).catch();
			} else {
				//We got an error while we are listening
				this.shutdownInfoServer().then(() => setTimeout(() => {
					if (!this.permanentlyClosed) {
						this.infoServer!.listen(this.config.VNODE_PROCESSORPORT);
					}
				}, timeout)).catch();
			}
		});

		this.infoServer.listen(this.config.VNODE_PROCESSORPORT);
	}

	/**
	 * Respond to an incomming request for information about the blockchain.
	 * @param res The response object to use
	 * @param data The data send with the request
	 */
	private respond(res: http.ServerResponse, ip: string, data: NodeRequest): void {
		let foundPeer = false;
		const now = Date.now();

		//See if we have seen this peer before and also remove old peers.
		for (let i = this.seenPeers.length - 1; i >= 0; i--) {
			if (this.seenPeers[i].ip === ip && this.seenPeers[i].port === data.port) {
				this.seenPeers[i].lastSeen = now;
				this.seenPeers[i].version = data.version;
				this.seenPeers[i].oldestCompVersion = data.oldestCompVersion;
				foundPeer = true;
			} else if (this.seenPeers[i].lastSeen + 1000 * 3600 * this.config.VNODE_REMEMBERPEER < now) {
				if (this.hasPeer(this.seenPeers[i].ip, this.seenPeers[i].port, true)) {
					this.seenPeers[i].lastSeen = now;
				} else {
					this.seenPeers.splice(i, 1);
				}
			}
		}
		//Add the peer if we haven't seen it before
		if (!foundPeer) {
			this.seenPeers.push({ ip, port: data.port, lastSeen: now, version: data.version, oldestCompVersion: data.oldestCompVersion });
		}

		//Add peers from the list of peers we have recently seen.
		const toCheck = this.seenPeers.slice(0); //Copy the array
		const newPeers: Array<{ ip: string, port: number }> = [];
		let foundConnected: boolean = false;
		while (toCheck.length > 0 && (newPeers.length < 5 || !foundConnected)) {
			const nextPeerNumber = Math.floor(toCheck.length * Math.random());
			const nextPeer = toCheck[nextPeerNumber];
			//If this peer is compatible and not the peer that is requesting
			if ((nextPeer.version >= data.oldestCompVersion && nextPeer.version <= data.version ||
				data.version >= nextPeer.oldestCompVersion && data.version <= nextPeer.version) &&
				(nextPeer.ip !== ip || nextPeer.port !== data.port)) {

				//We only add the peer if we don't have enough yet or if we did not found a connected peer yet
				let shouldAdd = newPeers.length < 5;
				for (const peer of this.peers) {
					if (nextPeer.ip === peer.ip && nextPeer.port === peer.listenPort) {
						foundConnected = true;
						shouldAdd = true;
					}
				}
				if (shouldAdd) {
					newPeers.unshift(nextPeer);
				}
			}
			toCheck.splice(nextPeerNumber, 1);
		}

		//Add ourselfs if the peer doesn't have enough peers yet or a small random chance
		if (newPeers.length < 5 || !foundConnected || Math.random() * this.seenPeers.length < 5) {
			if (Client.version >= data.oldestCompVersion && Client.version <= data.version ||
				data.version >= Client.oldestCompatibleVersion && data.version <= Client.version) {

				newPeers.unshift({ ip: "processor", port: this.port! });
				//Disconnect from one of our peers if we reached the limit
				if (this.peers.length === this.config.VNODE_MAXPEERS) {
					this.peers[Math.floor(this.peers.length * Math.random())].disconnect();
				}
			}
		}

		//Construct the response
		const responseJson: ProcessorResponse = {
			nodeVersion: this.config.VNODE_NODEVERSION,
			postgresVersion: this.postgresVersion,
			signPrefix: Crypto.binaryToUtf8(this.signPrefix),
			blockInterval: this.config.VNODE_BLOCKINTERVAL,
			blockMaxSize: this.config.VNODE_MAXBLOCKSIZE,
			latestBlock: this.latestExistingBlock,
			externalIp: ip,
			peers: newPeers
		};

		let responseData = Crypto.utf8ToBinary(JSON.stringify(responseJson));
		//If needed encrypt the response
		if (this.config.VNODE_ENCRYPTIONKEY !== "") {
			const sendingIV = encryption.randomBytes(16);
			const sendingCipher = encryption.createCipheriv("AES-256-CTR", this.encryptionKey!, sendingIV);
			responseData = Buffer.concat([sendingIV, sendingCipher.update(responseData)]);
			sendingCipher.final();
		}
		//Send the response
		res.writeHead(200);
		res.end(responseData);
	}

	/**
	 * Load the certificate from the location found in the config file (if any).
	 * Returns undefined if it failed to load the certificate.
	 */
	private loadCertificate(): { key: Buffer, cert: Buffer } | undefined {
		try {
			return {
				key: FS.readFileSync(this.config.VNODE_KEYPATH),
				cert: FS.readFileSync(this.config.VNODE_CERTPATH)
			};
		} catch (error) {
			//Do not log error as it may contain the certificate key.
			Log.error(`Failed to load certificate at: key: ${this.config.VNODE_KEYPATH} and cert: ${this.config.VNODE_CERTPATH}.`);
			return undefined;
		}
	}

	/** Shuts down the server. */
	public async shutdownInfoServer(): Promise<void> {
		if (this.isClosingInfoServer || this.infoServer === undefined) {
			if (this.permanentlyClosed) {
				return Promise.resolve();
			} else {
				return Promise.reject(new Error("Server already closing."));
			}
		} else {
			this.isClosingInfoServer = true;
			return new Promise<void>((resolve) => {
				this.infoServer!.close(() => {
					this.isClosingInfoServer = false;
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
		if (this.infoServer !== undefined && this.infoServer.listening) {
			this.isClosingInfoServer = true;
			promises.push(this.shutdownInfoServer());
		}
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