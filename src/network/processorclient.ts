/*!
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
import { Client as PGClient } from "pg";
import { Config } from "../config";
import { Client, NodeRequest, ProcessorResponse } from "./client";
import { Peer } from "./peer";

/**
 * The processor client is the network client for the processor.
 * Unlike the other nodes it does not request new block or peers, only provide them.
 * In addition it is the starting point for other peers to find the first peers.
 */
export class ProcessorClient extends Client {
	protected postgresVersion: number = 0;
	protected infoServer: http.Server | https.Server | undefined;
	protected isClosingInfoServer: boolean = false;
	protected seenPeers: Array<{ ip: string, port: number, lastSeen: number, version: number, oldestCompVersion: number, IPv6: boolean }> = [];
	protected watchingCert: boolean = false;
	protected isReloadingCert: boolean = false;
	protected notificationsClient: PGClient;
	private cachingBlocksFailures: number = 0;

	/** Create a new client for the processor. */
	constructor(worker: Worker, config: Readonly<Config>) {
		super(worker, config);
		this.signPrefix = Crypto.utf8ToBinary(this.config.VNODE_SIGNPREFIX);
		this.latestExistingBlock = this.config.VNODE_LATESTEXISTINGBLOCK;
		this.ip = "processor";

		const user = this.config.VNODE_ISPROCESSOR ? this.config.VNODE_DBUSER : this.config.VNODE_DBUSER_NETWORK;
		const password = this.config.VNODE_ISPROCESSOR ? this.config.VNODE_DBPASSWORD : this.config.VNODE_DBPASSWORD_NETWORK;
		this.notificationsClient = new PGClient({
			user,
			database: this.config.VNODE_DBNAME,
			password,
			port: this.config.VNODE_DBPORT,
			host: this.config.VNODE_DBHOST
		}).on("error", (error) => {
			error.message = error.message.replace(new RegExp(password, "g"), "");
			Log.warn("Problem with database connection.", error);
		}).on("end", () => {
			setTimeout(() => this.listen(), 5000);
		}).on("notification", (notification) => {
			const payload: { block?: number, ts: number, txs?: number, other: number } = JSON.parse(notification.payload!);
			if (payload.block !== undefined) {
				this.cacheBlocks();
			}
		});

		//Create the peer server (but do not yet start listening, this will create the info server once it starts listening)
		this.createPeerServer();

		//Retrieve the database version
		this.getDatabaseVersion().then(async (version) => {
			this.postgresVersion = version;
			//Retrieve the last known block from the database
			this.latestRetrievedBlock = await this.getLatestRetrievedBlock();
			if (this.latestRetrievedBlock > this.latestExistingBlock) {
				this.latestExistingBlock = this.latestRetrievedBlock;
			}
			//Update with new blocks whenever available
			await this.cacheBlocks();
			this.listen();

			//Start the peer server
			this.server!.listen(this.port);
		});

		//If everything is up and running report back memory usage, otherwise we try again later...
		setInterval(() => {
			if (this.server !== undefined && this.server.listening && !this.isClosingServer &&
				this.infoServer !== undefined && this.infoServer.listening && !this.isClosingInfoServer) {
				const memory = process.memoryUsage();
				this.worker.send({ type: "report", memory: (memory.heapTotal + memory.external) / 1024 / 1024 });
			}
		}, 20000);
	}

	private async listen(): Promise<void> {
		try {
			await this.notificationsClient.connect();
		} catch (error) {
			//End will be called, which will create a new connection in a moment.
		}
		try {
			await this.notificationsClient.query("LISTEN blocks;");
		} catch (error) {
			//End will be called, which will create a new connection in a moment.
			await this.notificationsClient.end().catch(() => { });
		}
	}

	/** Will be called when a peer disconnects. */
	private onDisconnect(peer: Peer): void {
		//A peer disconnected
		this.peers.splice(this.peers.indexOf(peer), 1);
		this.disconnectedPeers.push(peer);
		setTimeout(() => this.disconnectedPeers.splice(this.disconnectedPeers.indexOf(peer), 1), this.config.VNODE_PEERTIMEOUT);
	}

	/**
	 * Get what database version is being used. It will retry until it succeeds.
	 * @param timeout The timeout after which to try again should it fail
	 */
	protected async getDatabaseVersion(timeout = 5000): Promise<number> {
		try {
			return (await this.query("SELECT current_setting('server_version_num');", [])).rows[0].current_setting;
		} catch (error) {
			Log.warn("Failed to retrieve database version", error);
			return new Promise<number>((resolve) => setTimeout(() => resolve(this.getDatabaseVersion(Math.min(timeout * 1.5, 300000))), timeout));
		}
	}

	/** Will get new blocks from the database, cache them and let peers know of new blocks. */
	protected async cacheBlocks(): Promise<void> {
		if (this.cachingBlocksFailures > 3) {
			Log.error("Is still caching blocks multiple times in a row.");
		}
		this.cachingBlocksFailures++;

		//Get the blocks
		let blocks: DBBlock[];
		try {
			blocks = (await this.query("SELECT * FROM basics.blocks WHERE block_id > $1 ORDER BY block_id;",
				[this.cachedBlocks.length === 0 ? this.latestRetrievedBlock - Client.CACHED_BLOCKS : this.latestRetrievedBlock])).rows;
		} catch (error) {
			//It will be tried again because of the setInterval
			return Log.warn("Failed to retrieve database info", error);
		}

		//Store the blocks in cache
		if (blocks.length > 0) {
			for (const block of blocks) {
				if (this.cachedBlocks.length === 0 || this.cachedBlocks[this.cachedBlocks.length - 1].id + 1 === block.block_id) {
					this.cachedBlocks.push(new Block(block));
				} else {
					Log.error("Retrieved cached blocks with too old/new ids.");
					this.cachedBlocks.splice(0);
					this.cachedBlocksStartId = -1;
				}
			}
			if (this.cachedBlocks.length > Client.CACHED_BLOCKS) {
				this.cachedBlocks.splice(0, this.cachedBlocks.length - Client.CACHED_BLOCKS);
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

		//We are finished caching blocks
		this.cachingBlocksFailures = 0;
	}

	/**
	 * Create a server for other peers to connect to, but do not yet start listening.
	 * Will also create the info server once it starts listening.
	 */
	private createPeerServer(): void {
		if (!this.isClosingServer || this.permanentlyClosed) {
			let timeout = 5000;

			//Setup a server for incoming connections.
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
				if (this.peers.length >= this.config.VNODE_MAXPEERS || this.isClosingServer) {
					socket.end("");
				} else {
					const peer = new Peer(this, socket);
					peer.on("disconnect", this.onDisconnect.bind(this));
					this.peers.push(peer);
				}
			});

			//Restart the server in a bit after an error.
			this.server.on("error", (error) => {
				Log.warn("Peer server error", error);
				if (!this.server!.listening) {
					timeout = Math.min(timeout * 1.5, 300000);
					//We got an error while starting up
					this.shutdownServer().catch(() => { }).then(() => setTimeout(() => {
						if (!this.permanentlyClosed) {
							this.server!.listen(this.port);
						}
					}, timeout));
				} else {
					//We got an error while we are listening
					this.shutdownServer().catch(() => { }).then(() => setTimeout(() => {
						if (!this.permanentlyClosed) {
							this.server!.listen(this.port);
						}
					}, timeout));
				}
			});
		}
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
									if ((this.server as any).setSecureContext instanceof Function) {
										//Available since node 11
										(this.server as any).setSecureContext(newCertificate);
									} else {
										//Not officially available, but it works anyway.
										(this.server as any)._sharedCreds.context.setCert(newCertificate.cert);
										(this.server as any)._sharedCreds.context.setKey(newCertificate.key);
									}
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

		//What to do if there is an incoming connection
		this.infoServer.on("request", (req: http.IncomingMessage, res: http.ServerResponse) => {
			if (req.method === "POST") {
				let body: string = "";
				//Read part of the body
				req.on("data", (postData) => {
					body += postData.toString();
					if (body.length > 10000) {
						res.writeHead(413);
						res.end("Payload too large.");
						req.socket.destroy();
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
					let ip: string;
					const forwarded = req.headers["x-forwarded-for"];
					if (forwarded instanceof Array) {
						ip = forwarded[0];
					} else if (typeof forwarded === "string") {
						const fwd = forwarded.split(",")[0].trim();
						const splitted = fwd.split(":");
						ip = splitted.length === 2 ? splitted[0] : fwd;
					} else if (typeof req.socket.remoteAddress === "string") {
						ip = req.socket.remoteAddress;
					} else if (typeof req.socket.remoteAddress === "string") {
						ip = req.socket.remoteAddress;
					} else {
						res.writeHead(400);
						res.end("Invalid ip.");
						return;
					}
					this.respond(res, ip, data);
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
				this.shutdownInfoServer().catch(() => { }).then(() => setTimeout(() => {
					if (!this.permanentlyClosed) {
						this.infoServer!.listen(this.config.VNODE_PROCESSORPORT);
					}
				}, timeout));
			} else {
				//We got an error while we are listening
				this.shutdownInfoServer().catch(() => { }).then(() => setTimeout(() => {
					if (!this.permanentlyClosed) {
						this.infoServer!.listen(this.config.VNODE_PROCESSORPORT);
					}
				}, timeout));
			}
		});

		this.infoServer.listen(this.config.VNODE_PROCESSORPORT);
	}

	/**
	 * Respond to an incoming request for information about the blockchain.
	 * @param res The response object to use
	 * @param data The data send with the request
	 */
	private respond(res: http.ServerResponse, ip: string, data: NodeRequest): void {
		let foundPeer = false;
		const now = Date.now();
		const isIPv6 = ip.indexOf(":") !== -1;

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
			this.seenPeers.push({ ip, port: data.port, lastSeen: now, version: data.version, oldestCompVersion: data.oldestCompVersion, IPv6: isIPv6 });
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
				nextPeer.IPv6 === isIPv6 && (nextPeer.ip !== ip || nextPeer.port !== data.port)) {

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
			/** Version processor is assumed to use. Will lead to a warning message if this is wrong. */
			nodeVersion: process.versions.node.split(".")[0],
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
		if (this.encryptionKey !== undefined) {
			const sendingIV = encryption.randomBytes(16);
			const sendingCipher = encryption.createCipheriv("AES-256-CTR", this.encryptionKey, sendingIV);
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

		//Close database connection
		promises.push(this.pool.end().catch((error) => Log.warn("Failed to properly shutdown database pool.", error)));

		await Promise.all(promises).catch(() => { });

		return process.exit(exitCode);
	}
}