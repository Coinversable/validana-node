/**
 * @license
 * Copyright Coinversable B.V. All Rights Reserved.
 *
 * Use of this source code is governed by a AGPLv3-style license that can be
 * found in the LICENSE file at https://validana.io/license
 */

import * as net from "net";
import * as encryption from "crypto";
import { Crypto, Log, Block } from "@coinversable/validana-core";
import { VObservable } from "../tools/observable";
import { Client } from "./client";

/** How an outstanding block request looks like. */
export interface RetrievingBlocks {
	start: number;
	amount: number;
	blocks: Block[];
}

/**
 * The different message types available in the exchange protocol.
 * Setup: 1 byte own version, 1 byte oldest compatible version, 4 bytes extension flags, 2 bytes listen port, (16 bytes encryption IV)
 * Setup Response: 1 byte own version, 4 bytes extension flags, (16 bytes encryption IV)
 * Next Needed: 8 bytes next needed block id
 * PeersRequest: no data
 * PeersResponse: (encrypted) json array of {ip:string, port:number}
 * BlocksRequest: 8 bytes startBlockId, 2 bytes amount of blocks.
 * BlocksResponse: (encrypted) list of blocks
 * NewIV: 16 bytes encryption IV
 */
enum Message { Setup = 0, SetupResponse = 1, NextNeeded = 2, PeersRequest = 3, PeersResponse = 4, BlocksRequest = 5, BlocksResponse = 6, NewIV = 7 }

/**
 * The peer represents another node that we can connect to.
 * Each peer keeps track of the download speed and the current protocol status.
 * It will update observers with completed block requests, list of new peers, new latest block id and disconnects.
 * Any updates about block requests will contain blocks validated to be signed by the processor.
 */
export class Peer extends VObservable<RetrievingBlocks | Array<{ ip: string, port: number }> | number> {
	private readonly client: Readonly<Client>;
	public readonly ip: string | "processor"; //Ip address of this peer
	private connection: net.Socket;
	private connectionIncomming: boolean; //Direction of the connection
	public readonly connectionPort: number; //Which port we are connected to
	public listenPort: number | undefined; //Which port the peer is listening on for incomming connections.
	public version: number | undefined; //Version of this peer
	public extensionFlags: Buffer | undefined; //Extension flags of this peer
	public latestKnownBlock: number = -1; //Latest block this peer has

	public disconnectTime: number | undefined; //At what time did we disconnect (or undefined if never)

	/** How fast do WE download from this peer. */
	public downloadSpeed: number | undefined; //An indication of the download speed (only if we download something large)
	private previousDataLength: number = 0;
	private currentData: Buffer[] = [];
	private currentDataLength = 0;
	private expectedDataLength = 0;

	/**
	 * Note that the encrypted data is malleable.
	 * Not a problem as we don't trust data send by other peers in the first place, but keep it in mind!
	 * We use encryption so those who don't participate have a hard time finding out who participates and what data is stored.
	 */
	private sendingIV: Buffer = Buffer.alloc(0);
	private sendingCipher: encryption.Cipher | undefined;
	private receivingCipher: encryption.Decipher | undefined;
	private refreshIV: number = 0; //When do we need to refresh the IV again.

	/** If we are currently setting up this should have a value. */
	private setupTimeout: NodeJS.Timer | undefined; //Time we give peer to respond to our request
	private isSetup: boolean = false; //Have we finished the setup
	private sendSetup: boolean = false; //Did we send the setup message
	private latestKnownTimeout: NodeJS.Timer | undefined; //Time we give peer to respond to our request
	public isRetrievingPeers: boolean = false; //Are we currently retrieving peers?
	public lastRetrievedPeers: number | undefined; //What was the last time we retrieved peers
	private retrievingPeersTimeout: NodeJS.Timer | undefined; //Time we give peer to respond to our request
	public retrievingBlocks: RetrievingBlocks | undefined; //The blocks we are currently retrieving (or undefined if none)
	private retrievingBlocksTimeout: NodeJS.Timer | undefined; //Time we give peer to respond to our request
	private outstandingMessages: Array<{ message: Message; data: Buffer }> = [];

	/** Create a new peer, either through an ip and port, after we can choose to connect, or through receiving an incomming connection. */
	constructor(client: Readonly<Client>, connection: net.Socket | { ip: string, port: number }) {
		super();
		this.client = client;
		this.addObserver(this.client);

		//Create encryption key if needed
		if (this.client.encryptionKey !== undefined) {
			this.sendingIV = encryption.randomBytes(16);
			this.sendingCipher = encryption.createCipheriv("AES-256-CTR", this.client.encryptionKey, this.sendingIV);
		}
		//Setup the connection or receive the connection
		if (connection instanceof net.Socket) {
			this.connectionIncomming = true;
			this.ip = connection.remoteAddress!;
			this.connectionPort = connection.remotePort!;
			this.connection = connection;
		} else {
			this.connectionIncomming = false;
			this.ip = connection.ip;
			//We are both connected to this port and it is the port the peer is listening on for incomming connections.
			this.connectionPort = connection.port;
			this.listenPort = connection.port;

			//If it is the processor connect to the processor host, otherwise connect to the ip given.
			this.connection = net.createConnection(this.connectionPort, this.ip === "processor" ? this.client.config.VNODE_PROCESSORHOST : this.ip);
		}

		//We often send small packets, so buffer them if there are outstanding packets with no ack yet.
		this.connection.setNoDelay(false);

		//Connect and setup
		this.connect();
	}

	/**
	 * Request up to numberOfBlocks from a peer if it is not currently requesting blocks.
	 * @param startBlock The block number to start with requesting
	 * @param numberOfBlocks The amount of blocks that you want (if the peer has that many)
	 */
	public requestBlocks(startBlock: number, numberOfBlocks: number): void {
		if (this.retrievingBlocks !== undefined) {
			return;
		}
		this.retrievingBlocks = {
			start: startBlock,
			amount: numberOfBlocks,
			blocks: []
		};

		//Send our block request
		this.send(Message.BlocksRequest, Crypto.uLongToBinary(startBlock), Crypto.uInt16ToBinary(numberOfBlocks));
	}

	/**
	 * Get which peers this peer has.
	 * If it is already requesting peers the new request will be ignored.
	 */
	public getPeers(): void {
		if (!this.isRetrievingPeers) {
			this.isRetrievingPeers = true;
			this.send(Message.PeersRequest);
		}
	}

	/** Let other peers know we have a new latest known block. */
	public newLatestKnown(latestKnownBlock: number): void {
		if (latestKnownBlock > this.latestKnownBlock) {
			this.send(Message.NextNeeded, Crypto.uLongToBinary(latestKnownBlock + 1));
		}
	}

	/**
	 * Deal with an incomming message from another peer for version 1 of the protocol.
	 * @param message The message type
	 * @param data The data in the message.
	 */
	private handleV1Message(message: Message, data: Buffer): void {
		switch (message) {
			case Message.NextNeeded:
				if (this.latestKnownTimeout !== undefined) {
					clearTimeout(this.latestKnownTimeout);
					this.latestKnownTimeout = undefined;
				}
				if (data.length < 8) {
					this.disconnect("Peer send invalid latest known data.");
					return;
				} else {
					const newLatestKnown = Crypto.binaryToULong(data.slice(0, 8)) - 1; //Next needed - 1
					if (!Number.isSafeInteger(newLatestKnown) || newLatestKnown < this.latestKnownBlock) {
						this.disconnect("Peer send invalid latest known value.");
						return;
					} else {
						//Notify client about this peer knowing a new block.
						this.latestKnownBlock = newLatestKnown;
						this.setChanged();
						this.notifyObservers(this.latestKnownBlock);
					}
				}
				break;
			case Message.PeersRequest:
				if (this.outstandingMessages.find((value) => value.message === Message.PeersRequest) !== undefined) {
					this.disconnect("Peer send too many peer requests.");
					return;
				}
				//Construct the response with all peers we have
				const myPeers: Array<{ ip: string, port: number }> = [];
				for (const peer of this.client.peers) {
					if (peer !== this && peer.listenPort !== undefined) {
						myPeers.push({ ip: peer.ip, port: peer.listenPort });
					}
				}
				this.send(Message.PeersResponse, this.encryptData(Crypto.utf8ToBinary(JSON.stringify(myPeers))));
				break;
			case Message.PeersResponse:
				if (!this.isRetrievingPeers) {
					this.disconnect("Peer sending peers we did not ask for.");
					return;
				}
				this.isRetrievingPeers = false;
				if (this.retrievingPeersTimeout !== undefined) {
					clearTimeout(this.retrievingPeersTimeout);
					this.retrievingPeersTimeout = undefined;
				}
				try {
					//Decrypt data if needed
					if (this.receivingCipher !== undefined) {
						data = this.receivingCipher.update(data);
					}
					const newPeers: Array<{ ip: string, port: number }> = JSON.parse(Crypto.binaryToUtf8(data));
					if (!(newPeers instanceof Array)) {
						this.disconnect("Peer send peers respond message with invalid array.");
						return;
					}

					//Ignore if we receive more than maxPeers peers at once
					const notifyPeers: Array<{ ip: string, port: number }> = [];
					for (const newPeer of newPeers) {
						if (!this.isValidIpAndPort(newPeer.ip, newPeer.port)) {
							this.disconnect("Peer send peers respond message with invalid peer port or ip.");
							return;
						} else {
							notifyPeers.push({ ip: newPeer.ip, port: newPeer.port });
						}
					}
					//Even if we found none, let the client know about that.
					this.lastRetrievedPeers = Date.now();
					this.setChanged();
					this.notifyObservers(notifyPeers);
				} catch (error) {
					this.disconnect("Peer send peers respond message with invalid json.");
					return;
				}
				break;
			case Message.BlocksRequest:
				if (this.outstandingMessages.find((value) => value.message === Message.BlocksRequest) !== undefined) {
					this.disconnect("Peer send too many block requests.");
					return;
				}
				if (data.length !== 10) {
					this.disconnect("Peer send blocks request message with invalid data.");
					return;
				}
				const start = Crypto.binaryToULong(data.slice(0, 8));
				//We send at most maxMessageBlocks blocks at the time, even if more were requested.
				const amount = Math.min(Crypto.binaryToUInt16(data.slice(8, 10)), this.client.config.VNODE_MAXSENDAMOUNT);
				if (!Number.isSafeInteger(start) || start < 0 || amount === 0) {
					this.disconnect("Peer send blocks request message with invalid start or amount.");
					return;
				}

				this.client.getBlocks(start, amount).then((result) => this.send(Message.BlocksResponse, this.prepareBlockForSending(result)));
				break;
			case Message.BlocksResponse:
				//Don't waste time on verifying blocks we did not ask for.
				if (this.retrievingBlocks === undefined) {
					this.disconnect("Peer send blocks while we didn't ask for any.");
					return;
				}
				if (this.retrievingBlocksTimeout !== undefined) {
					clearTimeout(this.retrievingBlocksTimeout);
					this.retrievingBlocksTimeout = undefined;
				}
				//Decrypt data if needed
				if (this.receivingCipher !== undefined) {
					data = this.receivingCipher.update(data);
				}
				let blocks: Block[];
				try {
					//Unmerge the blocks from the binary data, will throw an error if the data is invalid
					blocks = Block.unmerge(data);
				} catch (error) {
					if (error.message === "Unsupported version.") {
						//We don't know if we don't understand the block format or peer simply send invalid blocks
						//as we cannot validate the blocks.
						Log.warn("Eighter peer send invalid blocks or a new block version is available and we need to update.");
					}
					this.disconnect("Peer send invalid blocks");
					return;
				}
				if (blocks.length > this.retrievingBlocks.amount) {
					this.disconnect("Peer send blocks we did not ask for.");
					return;
				}

				//Verify the blocks indeed come from the processor.
				for (const block of blocks) {
					if (!block.verifySignature(this.client.signPrefix, this.client.processorPubKey!)) {
						this.disconnect("Peer send blocks not from the processor.");
						return;
					}
				}

				//Note that the response may not contain all the blocks we requested as the other peer may not have all of them (anymore).
				blocks.sort((a, b) => a.id - b.id);
				for (const block of blocks) {
					if (this.retrievingBlocks.start <= block.id && block.id < this.retrievingBlocks.start + this.retrievingBlocks.amount) {
						this.retrievingBlocks.blocks[block.id - this.retrievingBlocks.start] = block;
					} else {
						this.disconnect("Peer send blocks we didn't ask for.");
						return;
					}
				}

				//Make sure client can request new blocks right away
				const response = this.retrievingBlocks;
				this.retrievingBlocks = undefined;
				this.setChanged();
				this.notifyObservers(response);
				break;
			case Message.NewIV:
				if (this.client.encryptionKey === undefined) {
					this.disconnect("Peer send new IV, but we don't use encryption.");
					return;
				}
				if (data.length !== 16) {
					this.disconnect("Peer send invalid new IV.");
					return;
				}
				this.receivingCipher = encryption.createDecipheriv("AES-256-CTR", this.client.encryptionKey, data);
				break;
			default:
				this.disconnect("Peer send message with invalid message type.");
		}
	}

	/**
	 * Prepare blocks for sending. This will limit the number of block so it stays below the maxMessageSize (minus required overhead).
	 * If will also encrypt the data.
	 * @param blocks the blocks to limit.
	 */
	private prepareBlockForSending(blocks: Block[]): Buffer {
		const toEncrypt: Buffer[] = [];

		let totalSize = 0;
		for (const block of blocks) {
			totalSize += block.data.length;
			//1 byte overhead for the message type, send at least 1 block
			if (totalSize > this.client.config.VNODE_MAXSENDSIZE - 1 && toEncrypt.length !== 0) {
				break;
			}
			toEncrypt.push(block.data);
		}
		return this.encryptData(Buffer.concat(toEncrypt));
	}

	/** Encrypt and data (if needed) and returns the encrypted data. Will also refresh the IV when needed. */
	private encryptData(data: Buffer): Buffer {
		if (this.sendingCipher === undefined) {
			return data;
		}
		//Needed every 2^32= 4294967296 blocks (We use some spare for the current message)
		if (this.refreshIV > 4200000000) {
			//Do any needed cleanup.
			this.sendingCipher.final();
			//Create a new IV
			this.sendingIV = encryption.randomBytes(16);
			this.sendingCipher = encryption.createCipheriv("AES-256-CTR", this.client.encryptionKey!, this.sendingIV);
			this.refreshIV = 0;
			this.send(Message.NewIV, this.sendingIV);
		}
		//aes-256 has a blocklength of 16 bytes or 128 bits (and a key length of 256 bits)
		this.refreshIV += Math.ceil(data.length / 16);
		return this.sendingCipher.update(data);
	}

	/**
	 * Check if a port and ip address are valid, as well as checking that they do not connect to ourself.
	 * @param ip The IP address to check, both IPv4 and IPv6 are valid.
	 * @param port The port to check.
	 */
	private isValidIpAndPort(ip: string, port: number): boolean {
		if (typeof ip !== "string" || !Number.isSafeInteger(port)) {
			return false;
		}
		//Check if the port is valid
		if (port <= 0 || port > 65535) {
			return false;
		}
		//Check if this is the special processor address
		if (ip === "processor") {
			return true;
		}

		let IPv6Parts = ip.split(":");
		const IPv4Parts = ip.split(".");
		//If it is a valid IPv6 Address
		if (IPv6Parts.length >= 3 && IPv6Parts.length <= 8) {
			//IPv4 mapped IPv6 address
			if (ip.startsWith("::ffff:")) {
				if (IPv6Parts.length === 4 && IPv4Parts.length === 4) {
					IPv4Parts[0] = IPv4Parts[0].slice("::ffff:".length);
					IPv6Parts = []; //We no longer have to check this.
				} else if (IPv6Parts.length === 5) {
					IPv4Parts[0] = (IPv6Parts[3].length < 3 ? 0 : Number.parseInt(`0x${IPv6Parts[3].slice(-4, -2)}`)).toString();
					IPv4Parts[1] = Number.parseInt(`0x${IPv6Parts[3].slice(-2)}`).toString();
					IPv4Parts[2] = (IPv6Parts[4].length < 3 ? 0 : Number.parseInt(`0x${IPv6Parts[4].slice(-4, -2)}`)).toString();
					IPv4Parts[3] = Number.parseInt(`0x${IPv6Parts[4].slice(-2)}`).toString();
				} else {
					//Invalid IPv4 mapped IPv6 address
					return false;
				}
			}
		} else if (IPv4Parts.length !== 4) {
			//Neighter valid IPv6 address, nor valid IPv4 Address
			return false;
		}

		//Check for valid IPv4 address
		if (IPv4Parts.length === 4) {
			//In case it was constructed from an IPv6 address
			ip = IPv4Parts.join(".");
			//If we have invalid numbers
			for (const part of IPv4Parts) {
				if (/^(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$/.exec(part) === null) {
					return false;
				}
			}
			//Do not allow loopback addresses for the port we are listening to.
			if ((IPv4Parts[0] === "127" || ip === this.client.ip) && port === this.client.port) {
				return false;
			}
			//Do not allow all 0s address
			if (IPv4Parts[0] === "0" && IPv4Parts[1] === "0" && IPv4Parts[2] === "0" && IPv4Parts[3] === "0") {
				return false;
			}
		}

		//Check for valid IPv6 address
		if (IPv6Parts.length >= 3) {
			//In case we had a IPv4 mapped IPv6 address
			ip = IPv6Parts.join(":");
			let foundDoubleColon = false;
			for (let i = 0; i < IPv6Parts.length; i++) {
				if (IPv6Parts[i] === "") {
					//Found the one :: that it may contain, if we already found one it is invalid
					if (foundDoubleColon) {
						return false;
					}
					//In case it is at the start or end the one next to it will also be empty, so ignore it, otherwise we found the one
					if (!(i === 0 && IPv6Parts[i + 1] === "") && !(i === IPv6Parts.length - 1 && IPv6Parts[i - 1] === "")) {
						foundDoubleColon = true;
					}
				} else {
					//Check that every part is valid
					if (/^[0-9a-fA-F]{1,4}$/.exec(IPv6Parts[i]) === null) {
						return false;
					}
				}
			}
			//Do not allow loopback address for the port we are listening to.
			const ipMinified = ip.replace(/[0|:]+/, "");
			if (((ip.endsWith("1") && ipMinified === "1") || ip === this.client.ip) && port === this.client.port) {
				return false;
			}
			//Do not allow all 0s address
			if (ipMinified === "") {
				return false;
			}
		}

		//Everything was valid
		return true;
	}

	/** Setup the connection with another peer. */
	private connect(): void {
		Log.debug(`Connect to peer ${this.ip}:${this.connectionPort}`);

		//We opened the connection, so send the setup message.
		if (!this.connectionIncomming) {
			this.connection.on("connect", () => {
				this.send(Message.Setup, Crypto.uInt8ToBinary(Client.version), //Our version
					Crypto.uInt8ToBinary(Client.oldestCompatibleVersion), //Oldest compatible version
					Client.extensionFlags, //Extension flags
					Crypto.uInt16ToBinary(this.client.port!), //The port we listen on for incomming connections
					this.sendingIV); //SendingIV (possibly empty)
				this.sendSetup = true;

				//Send latest known block right away
				this.send(Message.NextNeeded, Crypto.uLongToBinary(this.client.latestRetrievedBlock + 1));

				//Give other peer 10-15 seconds to complete setup and provide latest known, which should be plenty.
				this.setupTimeout = setTimeout(() => this.disconnect("Peer not responding to our setup."), 10000);
				this.latestKnownTimeout = setTimeout(() => this.disconnect("Peer not giving latest known."), 15000);
			});
		} else {
			//Give other peer 10-15 seconds to start setup and provide latest known, which should be plenty.
			this.setupTimeout = setTimeout(() => this.disconnect("Peer not setting up."), 10000);
			this.latestKnownTimeout = setTimeout(() => this.disconnect("Peer not giving latest known."), 15000);
		}

		//If we receive more data
		this.connection.on("data", (newData) => {
			//Add the data
			this.currentData.push(newData);
			this.currentDataLength += newData.length;

			//Check if it is enough data to be useful
			while (this.currentDataLength - 4 >= this.expectedDataLength) {

				//If we have a new message (and thus no expectedDataLength)
				if (this.expectedDataLength === 0) {
					this.currentData = [Buffer.concat(this.currentData)];
					//First 4 bytes are expected data length
					this.expectedDataLength = Crypto.binaryToUInt32(this.currentData[0].slice(0, 4));

					if (this.expectedDataLength > Math.max(this.client.config.VNODE_MAXRECEIVESIZE, this.client.blockMaxSize + 10000)) {
						this.disconnect("Peer wants to send too large of a message.");
						return;
					}
				}

				//If we received a full message
				if (this.currentDataLength >= this.expectedDataLength - 4) {
					this.currentData = [Buffer.concat(this.currentData)];
					//First 4 bytes are expected data length, after that is data
					const data = this.currentData[0].slice(4, this.expectedDataLength + 4);
					this.currentData = [this.currentData[0].slice(this.expectedDataLength + 4)];
					this.currentDataLength -= this.expectedDataLength + 4;
					this.expectedDataLength = 0;

					//First byte tells us the message type
					const message = Crypto.binaryToUInt8(data.slice(0, 1));

					//Handle the message type.
					if (message === Message.Setup) {
						if (this.isSetup) {
							this.disconnect("Peer send setup while setup is already finished.");
							return;
						} else if (!this.connectionIncomming) {
							this.disconnect("Peer send setup, while we are doing the setup.");
							return;
						} else if (data.length < 9 || (data.length < 25 && this.client.encryptionKey !== undefined)) {
							this.disconnect("Peer send invalid setup message.");
							return;
						} else {
							this.isSetup = true;
							this.version = Crypto.binaryToUInt8(data.slice(1, 2));
							const oldestCompatibleVersion = Crypto.binaryToUInt8(data.slice(2, 3));
							this.extensionFlags = data.slice(3, 7);
							this.listenPort = Crypto.binaryToUInt16(data.slice(7, 9));
							if (this.client.encryptionKey !== undefined) {
								this.receivingCipher = encryption.createDecipheriv("AES-256-CTR", this.client.encryptionKey, data.slice(9, 25));
							}
							if (this.setupTimeout !== undefined) {
								clearTimeout(this.setupTimeout);
								this.setupTimeout = undefined;
							}
							if (this.listenPort === 0) {
								this.disconnect("Peer send an invalid listen port.");
								return;
							} else {
								//Send our setup response
								this.send(Message.SetupResponse, Crypto.uInt8ToBinary(Client.version), Client.extensionFlags, this.sendingIV);
								this.sendSetup = true;
								if (Client.version < oldestCompatibleVersion) {
									this.disconnect("Peer is using a newer version that is not compatible with ours.");
									return;
								}
								//Send latest known block
								this.send(Message.NextNeeded, Crypto.uLongToBinary(this.client.latestRetrievedBlock + 1));
							}
						}
					} else if (message === Message.SetupResponse) {
						if (this.isSetup) {
							this.disconnect("Peer send setup while setup is already finished.");
							return;
						} else if (this.connectionIncomming) {
							this.disconnect("Peer send setup response, while we should be doing the setup response.");
							return;
						} else if (data.length < 6 || (data.length < 22 && this.client.encryptionKey !== undefined)) {
							this.disconnect("Peer send invalid setup response.");
							return;
						} else {
							this.isSetup = true;
							this.version = Crypto.binaryToUInt8(data.slice(1, 2));
							this.extensionFlags = data.slice(2, 6);
							if (this.client.encryptionKey !== undefined) {
								this.receivingCipher = encryption.createDecipheriv("AES-256-CTR", this.client.encryptionKey, data.slice(6, 22));
							}
							if (this.setupTimeout !== undefined) {
								clearTimeout(this.setupTimeout);
								this.setupTimeout = undefined;
							}
							if (this.version < Client.oldestCompatibleVersion) {
								this.disconnect("Peer uses a version that we no longer support.");
								return;
							}
						}
					} else if (!this.isSetup) {
						this.disconnect("Peer is sending normal messages before setup is complete.");
						return;
					} else {
						this.handleV1Message(message, data.slice(1));
					}
				}
			}
		});

		this.connection.on("error", (error) => {
			//Peer is/went offline, not an error
			if (error.message.indexOf("ECONNREFUSED") === -1 && error.message.indexOf("ECONNRESET") === -1) {
				Log.warn(`Connection to peer ${this.ip}:${this.connectionPort} errored`, error);
			}
		});

		this.connection.on("close", () => {
			Log.debug(`Closed connection to peer ${this.ip}:${this.connectionPort}`);
			if (this.setupTimeout !== undefined) {
				clearTimeout(this.setupTimeout);
				this.setupTimeout = undefined;
			}
			if (this.retrievingPeersTimeout !== undefined) {
				clearTimeout(this.retrievingPeersTimeout);
				this.retrievingPeersTimeout = undefined;
			}
			if (this.retrievingBlocksTimeout !== undefined) {
				clearTimeout(this.retrievingBlocksTimeout);
				this.retrievingBlocksTimeout = undefined;
			}
			this.disconnectTime = Date.now();
			this.setChanged();
			this.notifyObservers();
		});

		/** Finished sending all messages, ready the next one. */
		this.connection.on("drain", () => {
			const message = this.outstandingMessages.splice(0, 1)[0];
			if (message !== undefined) {
				if (message.message === Message.PeersRequest) {
					//In case of a peer request message give 10 seconds.
					this.retrievingPeersTimeout = setTimeout(() => this.disconnect("Peer not responding to our peer request."), 10000);
				} else if (message.message === Message.BlocksRequest) {
					//Time how long it takes for the data to arive.
					this.previousDataLength = 0;
					this.retrievingBlocksTimeout = setInterval(() => {
						//We want at least minDownloadSpeed*1000 B/s download speed, otherwise we drop this peer.
						//Does not take RTT in account, but first interval takes 10 seconds, so shouldn't matter too much.
						const downloadSpeed = (this.currentDataLength - this.previousDataLength) * this.client.config.VNODE_MINDOWNLOADSPEED / 10;
						this.previousDataLength = this.currentDataLength;
						if (downloadSpeed < this.client.config.VNODE_MINDOWNLOADSPEED) {
							this.disconnect("Peer is sending data too slow.");
							return;
						}
					}, 10000);
				}
				//If it managed to fully drain this will be true
				if (this.connection.write(message.data)) {
					this.connection.emit("drain");
				}
			}
		});
	}

	/**
	 * Called if we disconnect, if the other party disconnects the connection.on("close") will be called instead.
	 * @param error The error why we disconnect, or none.
	 */
	public async disconnect(error: string = ""): Promise<void> {
		if (this.disconnectTime === undefined) {
			if (error !== "") {
				Log.warn(error);
			}

			this.disconnectTime = Date.now();
			await new Promise((resolve) => this.connection.end("", resolve));
			//The close event will be called once the connection has closed.
		}
	}

	/**
	 * Send a message to another client.
	 * @param message The message to send
	 * @param data The data in this message.
	 */
	private send(message: Message, ...data: Buffer[]): void {
		if (this.disconnectTime === undefined) {
			let totalLength = 1; //Length of 'message' field
			for (const dataPart of data) {
				totalLength += dataPart.length;
			}
			const totalData = Buffer.concat([Crypto.uInt32ToBinary(totalLength), Crypto.uInt8ToBinary(message), ...data]);

			//Send requests first so other peer can start prepairing/responding while receiving data.
			//Also note that any encypted data (peer/block response) and NewIV should be send in order!
			if (message === Message.Setup || message === Message.SetupResponse || message === Message.BlocksRequest || message === Message.PeersRequest) {
				this.outstandingMessages.unshift({ message, data: totalData });
			} else {
				this.outstandingMessages.push({ message, data: totalData });
			}
		}
		//Make sure it starts sending the next message if the buffer is empty, otherwise wait till the buffer is empty.
		if (this.connection.bufferSize === 0 && (this.sendSetup || message === Message.Setup || message === Message.SetupResponse)) {
			this.connection.emit("drain");
		}
	}
}