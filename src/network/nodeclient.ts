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
import * as encryption from "crypto";
import { Worker } from "cluster";
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
				const memory = process.memoryUsage();
				this.worker.send({ type: "report", memory: (memory.heapTotal + memory.external) / 1024 / 1024 });
			}
		}, 20000);
	}

	/** Create a server for other peers to connect to. It will also retrieve info from the processor once it has started. */
	private createServer(): void {
		let timeout = 5000;

		this.server = net.createServer();
		this.server.maxConnections = this.config.VNODE_MAXPEERS;

		this.server.on("listening", async () => {
			timeout = 5000;
			//In case no port was specified we set it to the port that was giving.
			this.port = (this.server!.address() as net.AddressInfo).port;

			//Get the processor info. Do not await, we can get incoming connections before the response returned.
			const retrieveProcessorPromise = this.retrieveProcessorInfo();

			//When we receive a new connection.
			this.server!.on("connection", async (socket) => {
				//Do not create the peer till we have the processor info, as we need this when communicating.
				await retrieveProcessorPromise;

				//Create a peer if we do not yet exceed max peers
				const addr = socket.address() as net.AddressInfo;
				if (this.peers.length >= this.config.VNODE_MAXPEERS || this.hasPeer(addr.address, addr.port)) {
					socket.end("");
				} else {
					const newPeer = this.createPeer(socket);
					this.peers.push(newPeer);
					if (this.peers.length < this.config.VNODE_MINPEERS) {
						newPeer.getPeers();
					}
				}
			});

			//Now we wait for the the info to be returned before getting more peers.
			await retrieveProcessorPromise;

			//Get more peers if needed.
			this.getPeers();
		});

		//Restart the server in a bit after an error.
		this.server.on("error", async (error) => {
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
			const query = "INSERT INTO basics.blocks SELECT * FROM json_populate_recordset(NULL::basics.blocks, $1::JSON);";
			const blocks = this.downloadBlocks[0].data.map((block) => ({
				block_id: block.id,
				version: block.version,
				previous_block_hash: "\\x" + block.getPreviousBlockHash().toString("hex"),
				processed_ts: block.processedTs,
				transactions: "\\x" + block.getTransactions().toString("hex"),
				transactions_amount: block.transactionsAmount,
				signature: "\\x" + block.getSignature().toString("hex")
			}));

			//Insert the block
			this.isStoringBlocks = true;
			try {
				await this.query(query, [JSON.stringify(blocks)]);
				//Notify of new blocks. If this fails it will try again next time it has blocks.
				const latestStoredBlock = Math.max(...blocks.map((block) => block.block_id));
				this.query(`NOTIFY downloaded, '${latestStoredBlock}'`, []).catch(() => { });

				this.cachedBlocks.push(...this.downloadBlocks[0].data as Block[]);
				if (this.cachedBlocksStartId === -1) {
					this.cachedBlocksStartId = this.downloadBlocks[0].start;
				}
				if (this.cachedBlocks.length > Client.CACHED_BLOCKS) {
					this.cachedBlocksStartId = this.cachedBlocksStartId + this.cachedBlocks.length - Client.CACHED_BLOCKS;
					this.cachedBlocks.splice(0, this.cachedBlocks.length - Client.CACHED_BLOCKS);
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
				if (error.code.startsWith("23")) {
					//This should only happen if the database is setup incorrectly or the processor signed an invalid message.
					await Log.fatal("Database setup incorrectly or processor signed invalid message.");
					return process.exit(50);
				} else if (error.code.startsWith("08")) {
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
		if (start < this.latestRetrievedBlock + 1 + this.config.VNODE_MAXOUTSTANDINGBLOCKS) {
			toDownload.push({ start, amount: this.latestRetrievedBlock + 1 + this.config.VNODE_MAXOUTSTANDINGBLOCKS - start });
		}

		//Sort peers starting with lowest latestKnownBlock
		this.peers.sort((a, b) => (a.latestKnownBlock - b.latestKnownBlock));
		//Find peers who can provide the blocks that still need to be downloaded.
		for (const block of toDownload) {
			for (const peer of this.peers) {
				if (peer.retrievingBlocks === undefined && block.start <= peer.latestKnownBlock) {
					/** Request at most 50 blocks at once from another peer. */
					const requestAmount = Math.min(block.amount, peer.latestKnownBlock - block.start + 1, Client.MAX_BLOCKS);
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
	private async retrieveProcessorInfo(timeout = 5000): Promise<void> {
		this.lastRetrievedProcessorInfo = Date.now();

		return new Promise((resolve) => {
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
					setTimeout(() => this.retrieveProcessorInfo(Math.min(timeout * 1.5, 300000)).then(resolve), timeout);
				});

				response.on("end", async () => {
					if (response.statusCode! >= 200 && response.statusCode! < 300) {
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
							await Log.fatal("Unable to decode response from the processor, make sure you use the right encryption key!");
							process.exit(51);
							return;
						}
						const query = "INSERT INTO basics.info (key, value) VALUES ('blockInterval', $1), ('processorNodeVersion', $2), ('signPrefix', $3), " +
							"('processorPostgresVersion', $4) ON CONFLICT ON CONSTRAINT info_pkey DO UPDATE SET value = $1 WHERE info.key = 'blockInterval';";
						const values = [body.blockInterval.toString(), body.nodeVersion, body.signPrefix, body.postgresVersion.toString()];
						this.query(query, values).catch((error) => {
							Log.warn("Failed to store retrieved processor information.", error);
							setTimeout(() => this.retrieveProcessorInfo(Math.min(timeout * 1.5, 300000)).then(resolve), timeout);
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
								//If we are more than 2 blocks + 5 seconds behind (and not downloading blocks right now):
								if (this.downloadBlocks.length === 0 && this.latestRetrievedBlock < this.latestExistingBlock - 2 - 5 / body.blockInterval) {
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
										this.peers.push(this.createPeer({ ip: peer.ip, port: peer.port }));
									}
								}
							}
							resolve();
						});
					} else {
						Log.warn(`Failed to retrieve processor info, result code: ${response.statusCode}`);
						setTimeout(() => this.retrieveProcessorInfo(Math.min(timeout * 1.5, 300000)).then(resolve), timeout);
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
				setTimeout(() => this.retrieveProcessorInfo(Math.min(timeout * 1.5, 300000)).then(resolve), timeout);
			});
			request.write(requestData);
			request.end();
		});
	}

	private createPeer(connection: net.Socket | { ip: string, port: number }): Peer {
		const peer = new Peer(this, connection);
		peer.on("disconnect", this.onDisconnect.bind(this));
		peer.on("latestKnown", this.onLatestKnown.bind(this));
		peer.on("blocks", this.onNewBlocks.bind(this));
		peer.on("peers", this.onNewPeers.bind(this));
		return peer;
	}

	/** Will be called when a peer disconnects. */
	private onDisconnect(peer: Peer): void {
		//A peer disconnected
		this.peers.splice(this.peers.indexOf(peer), 1);
		this.disconnectedPeers.push(peer);
		setTimeout(() => this.disconnectedPeers.splice(this.disconnectedPeers.indexOf(peer), 1), this.config.VNODE_PEERTIMEOUT);

		//Get new peers now that we have one less
		this.getPeers();

		//Remove all outstanding downloads from this peer.
		for (let i = this.downloadBlocks.length - 1; i >= 0; i--) {
			if (this.downloadBlocks[i].data === peer) {
				this.downloadBlocks.splice(i, 1);
			}
		}
		//Retrieve new blocks (that may have been requested by this peer but no longer are)
		this.storeAndGetBlocks();
	}

	/** Will be called when a peer has a new block. */
	private onLatestKnown(peer: Peer, blockId: number): void {
		//Received a new highest block, check if we still need this block and aren't already requesting blocks from this peer.
		if (blockId > this.latestRetrievedBlock && peer.retrievingBlocks === undefined) {
			this.storeAndGetBlocks();
		}
	}

	/** Will be called when there are new peers available. */
	private onNewPeers(peers: Array<{ ip: string, port: number }>): void {
		//Received new peers to connect to
		for (const newPeerInfo of peers) {
			//Check if we have enough peers (allow a few more peers then needed so we don't redo this every disconnect)
			if (this.peers.length > (this.config.VNODE_MINPEERS + this.config.VNODE_MAXPEERS) / 2) {
				break;
			}
			if (!this.hasPeer(newPeerInfo.ip, newPeerInfo.port)) {
				this.peers.push(this.createPeer({ ip: newPeerInfo.ip, port: newPeerInfo.port }));
			}
		}
		//Get even more peers if needed
		this.getPeers();
	}

	/** Will be called when a peer has retrieved blocks. */
	private onNewBlocks(peer: Peer, blocks: RetrievingBlocks): void {
		//BlockRequest has been (partially) fulfilled
		const downloadBlocksGroups = [];
		let downloadBlocks: DownloadBlocks = {
			start: blocks.start,
			amount: 0,
			data: []
		};

		//Split it into multiple DownloadBlocks with consecutive blocks
		for (let i = 0; i < blocks.amount; i++) {
			if (blocks.blocks[i] !== undefined) {
				(downloadBlocks.data as Block[]).push(blocks.blocks[i]);
				downloadBlocks.amount++;
			} else {
				if (downloadBlocks.amount > 0) {
					downloadBlocksGroups.push(downloadBlocks);
				}
				downloadBlocks = {
					start: blocks.start + i + 1,
					amount: 0,
					data: []
				};
			}
		}
		if (downloadBlocks.amount > 0) {
			downloadBlocksGroups.push(downloadBlocks);
		}
		//Put them all into downloadBlocks and sort it.
		const filledRequest = this.downloadBlocks.findIndex((downloadBlock) => downloadBlock.data === peer);
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