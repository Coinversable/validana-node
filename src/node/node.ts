/**
 * @license
 * Copyright Coinversable B.V. All Rights Reserved.
 *
 * Use of this source code is governed by a AGPLv3-style license that can be
 * found in the LICENSE file at https://validana.io/license
 */

import * as Cluster from "cluster";
import {
	Crypto, Log, Basic, QueryStatus, TxStatus, DBBlock, Block, Transaction, CreatePayload, DeletePayload, PublicKey
} from "validana-core";
import { Config } from "../config";

/** The node is responsible for validating all blocks and transactions as well as building the current state depending on the transactions. */
export class Node extends Basic {

	//Queries for various actions
	private readonly versionQuery = "SELECT current_setting('server_version_num');";
	private readonly getInfo = "SELECT * FROM basics.info;";
	private readonly updateProgress = "INSERT INTO basics.info (key, value) VALUES ('currentBlock', $1), ('currentTransaction', $2) "
		+ "ON CONFLICT ON CONSTRAINT info_pkey DO UPDATE SET value = EXCLUDED.value;";
	private readonly getNextBlocks = "SELECT * FROM basics.blocks WHERE block_id >= $1 ORDER BY block_id ASC LIMIT $2;";
	private readonly getPreviousBlock = "SELECT * FROM basics.blocks WHERE block_id = $1;";
	private readonly insertTx = "INSERT INTO basics.transactions (version, transaction_id, contract_hash, valid_till, payload, public_key, signature, " +
		"status, processed_ts, block_id, position_in_block, sender, message, contract_type, receiver, extra1, extra2) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);";
	private readonly beginQuery = "BEGIN;";
	private readonly commitQuery = "COMMIT;";
	private readonly rollbackQuery = "ROLLBACK;";

	//Information needed for processing blocks and transactions
	private processingFailures = 0; //How many times in a row it failed to process a block.
	private transactionFailures = 0; //How many times in a row it failed to process a transaction
	private justConnected = true; //Did we just connect to the DB or not?
	private shouldRollback = false; //Should we rollback a transaction we were previously doing?
	private processedFirstBlock = false;
	private verifiedVersion = false;
	private notifiedGreaterTime = false;

	//Information about the previous block that was processed and the current block to be processed
	private previousBlock: Block | undefined;
	private currentBlock: Block | undefined;

	//Information from the database
	private blockInterval: number | undefined;
	private processorAddress: string;
	private processorNodeVersion: string | undefined;
	private processorPostgresVersion: number | undefined;
	private currentBlockId: number = 0;
	private currentTransaction: number = 0;

	//private static config: Readonly<Config>;
	private readonly worker: Cluster.Worker;

	/**
	 * Create the node.
	 * @param worker The worker that created the node
	 * @param config The config for the worker to use
	 */
	constructor(worker: Cluster.Worker, config: Readonly<Config>) {
		super({
			user: config.VNODE_DBUSER,
			database: config.VNODE_DBNAME,
			password: config.VNODE_DBPASSWORD,
			port: config.VNODE_DBPORT,
			host: config.VNODE_DBHOST
		});

		//Node.config = config;
		this.worker = worker;
		this.processorAddress = new PublicKey(Crypto.hexToBinary(config.VNODE_PUBLICKEY)).getAddress();
	}

	/**
	 * Process several blocks worth of transactions.
	 * It will retrieve the blocks from the database, validate them, validate the transactions inside them and process those transactions.
	 */
	public async processBlocks(): Promise<void> {

		//If we had multiple failures in a row the problem seems to not resolve itsself. We will still try to process again...
		if (this.processingFailures > 10) {
			Log.error("Node failed to process blocks multiple times in a row.");
		}

		//Connect to the DB (if it isn't connected already). This will set justConnected to true if needed.
		await this.connect();
		let result: QueryStatus;

		//Rollback old in progress transactions (won't emit an error if there is nothing to roll back!)
		if (this.justConnected || this.shouldRollback) {
			if ((result = await this.query(this.rollbackQuery, [])).error !== undefined) {
				return this.abortProcessing("Failed to rollback transactions after reconnecting.", true, result.error);
			}

			//Get all smart contracts (again) as we may have just rolled back a contract we added/deleted.
			const loadError = await this.loadSmartContracts();
			if (loadError !== undefined) {
				return this.abortProcessing("Failed to retrieve smart contracts.", false, loadError);
			}

			this.shouldRollback = false;
		}

		//Retrieve information required for this blockchain
		if (this.justConnected) {
			if ((result = await this.query(this.getInfo, [])).error !== undefined) {
				return this.abortProcessing("Failed to retrieve info transactions.", false, result.error);
			} else {
				//Check if the p2p already retrieved the info from the processor, if not return and try again later.
				if (result.rows.length === 0) {
					return this.abortProcessing("Still haven't retrieved info from processor.", false);
				}
				for (const row of result.rows as Array<{ key: string, value: string }>) {
					try {
						//Fill in information from the database.
						switch (row.key) {
							case "blockInterval":
								this.blockInterval = Number.parseInt(row.value, 10);
								break;
							case "processorNodeVersion":
								this.processorNodeVersion = row.value;
								break;
							case "signPrefix":
								this.signPrefix = Crypto.utf8ToBinary(row.value);
								break;
							case "processorPostgresVersion":
								this.processorPostgresVersion = Number.parseInt(row.value, 10);
								break;
							case "currentBlock":
								this.currentBlockId = Number.parseInt(row.value, 10);
								break;
							case "currentTransaction":
								this.currentTransaction = Number.parseInt(row.value, 10);
								break;
							default:
								Log.warn(`Unknown database key: ${row.key}`);
						}
					} catch (error) {
						return this.abortProcessing("Failed to load info from database.", false, error);
					}
				}
				//Either everything or nothing should be filled in at this point.
				if (this.blockInterval === undefined || this.processorNodeVersion === undefined ||
					this.signPrefix === undefined || this.processorPostgresVersion === undefined) {

					return this.abortProcessing("One or more keys not set after loading info from db.", false);
				}
			}
		}

		if (!this.verifiedVersion) {
			//Check if we are running the required nodejs version
			const version: number[] = [];
			for (const subVersion of process.versions.node.split(".")) {
				version.push(Number.parseInt(subVersion, 10));
			}
			const configVersion: string[] = this.processorNodeVersion!.split(".");
			for (let i = 0; i < configVersion.length || i < 1; i++) {
				if (!(configVersion[i] === "x" || version[i] === Number.parseInt(configVersion[i], 10))) {
					await Log.fatal(`Invalid node js version, running version: ${process.versions.node}, required version by processor: ${this.processorNodeVersion!}. Exiting process.`);
					return await Node.shutdown(53);
				}
			}
			//It should be compatible but we still log it just in case.
			Log.options.tags!.requiredNodeVersion = this.processorNodeVersion!;

			//Check if we are running the required postgres version
			if ((result = await this.query(this.versionQuery, [])).error !== undefined) {
				return this.abortProcessing("Failed to verify postgres version.", false, result.error);
			} else {
				const ourPostgresVersion = Number.parseInt(result.rows[0].current_setting, 10);
				if (Number.isNaN(ourPostgresVersion) || ourPostgresVersion < 90500) {
					await Log.fatal("Too old or invalid postgres version, requires at least 9.5, shutting down.");
					return await Node.shutdown(52);
				} else {
					if (ourPostgresVersion >= 110000) {
						Log.warn("The blockchain has not been tested for postgres version 11 or later, use at your own risk!");
					}
					//Up to postgres 10 a 0.1 version difference means a breaking change.
					if ((ourPostgresVersion < 100000 && Math.abs(ourPostgresVersion - this.processorPostgresVersion!) >= 10)
						|| (ourPostgresVersion >= 100000 && Math.abs(ourPostgresVersion - this.processorPostgresVersion!) >= 1000)) {

						const requiredVersion = Math.floor(this.processorPostgresVersion! / 10000) + "." + Math.floor((this.processorPostgresVersion! % 1000) / 100)
							+ "." + (this.processorPostgresVersion! % 10);
						const ourVersion = Math.floor(ourPostgresVersion / 10000) + "." + Math.floor((ourPostgresVersion % 1000) / 100)
							+ "." + (ourPostgresVersion % 10);
						await Log.fatal(`Postgres version does not match that of the processor, required version is ^${requiredVersion}, ` +
							`while we have version ${ourVersion}`);
						return await Node.shutdown(53);
					}

					//It should be compatible but we still log it just in case.
					Log.options.tags!.postgresVersion = result.rows[0].current_setting;
					Log.options.tags!.requiredPostgresVersion = "" + this.processorPostgresVersion!;
					this.verifiedVersion = true;
				}
			}
		}

		//If we just connected to the DB we don't know the current state, so get everything again.
		if (this.justConnected) {
			//Retrieve previous block information
			if (this.currentBlockId !== 0) {
				if ((result = await this.query(this.getPreviousBlock, [this.currentBlockId - 1])).error !== undefined) {
					return this.abortProcessing("Failed to retrieve previous block.", false, result.error);
				} else if (result.rows.length === 0) {
					//Unlikely this will recover itsself, which will cause it to start reporting errors instead
					return this.abortProcessing(`Block ${this.currentBlockId - 1} not found in the database.`, false);
				} else {
					try {
						this.previousBlock = new Block(result.rows[0] as DBBlock);
					} catch (error) {
						await Log.fatal("Failed to construct block from database info.", error);
						return await Node.shutdown(55);
					}
				}
			}
		}

		//Retrieve and validate the blocks
		let processedBlocks = 0;
		const processingTime = Date.now();
		let outOfTime: boolean = false;
		//Select 10 blocks at once (if possible)
		if ((result = await this.query(this.getNextBlocks, [this.currentBlockId, 10], "getBlocks")).error !== undefined) {
			return this.abortProcessing("Failed to load blocks.", false, result.error);
		} else {
			for (const block of result.rows as DBBlock[]) {
				processedBlocks++;
				try {
					this.currentBlock = new Block(block);
				} catch (error) {
					await Log.fatal(`Failed to construct block ${this.currentBlockId} from database block.`, error);
					return await Node.shutdown(55);
				}

				//No need to validate block signature again, the network part will not accept blocks that are not from the processor.
				//if (!this.currentBlock.verifySignature(this.signPrefix!, this.processorPublicKey!)) {}

				if (!this.currentBlock.verifyWithPreviousBlock(this.signPrefix!, this.previousBlock)) {
					//If the previous block hash does not match or the processed time goes backwards
					await Log.fatal(`Processor created invalid block ${this.currentBlock.id}, timestamp or `
						+ `previous block hash do not match the previous block.`, undefined);
					return await Node.shutdown(55);
				} else if (this.currentBlock.processedTs > Date.now() + 900000 && !this.notifiedGreaterTime) {
					//The block has been created at least 15 min in the future compared to this node
					this.notifiedGreaterTime = true;
					Log.warn(`Processor created block ${this.currentBlock.id} that it claims has been processed in the future. ` +
						`Our time: ${Math.floor(Date.now())}, block processed time: ${this.currentBlock.processedTs}`);
				} else if (this.previousBlock !== undefined && this.currentBlock.processedTs > this.previousBlock.processedTs + this.blockInterval! * 2000 + 5000) {
					//The block was created with a significant time difference compared to the previous node.
					Log.warn(`Processor created block ${this.currentBlock.id} with a significant later timestamp then the previous block. `
						+ `Previous block timestamp: ${this.previousBlock.processedTs}, this block timestamp: ${this.currentBlock.processedTs}`);
				}

				let transactionsToProcess: Transaction[];
				try {
					transactionsToProcess = Transaction.unmerge(this.currentBlock.getTransactions());
				} catch (error) {
					await Log.fatal(`Failed to contruct transactions found in block ${this.currentBlock.id}.`, error);
					return await Node.shutdown(55);
				}

				const previousBlockTs = this.previousBlock === undefined ? 0 : this.previousBlock.processedTs;
				const previousBlockHash = this.previousBlock === undefined ? Buffer.alloc(32, 0) : this.previousBlock.getHash(this.signPrefix!);
				for (let i = this.currentTransaction; i < transactionsToProcess.length; i++) {

					const transaction = transactionsToProcess[i];
					//We check the signature seperately for better error messages.
					if (!transaction.verifySignature(this.signPrefix!)) {
						await Log.fatal(`Invalid signature for transaction: ${Crypto.binaryToHex(transaction.getId())}, `
							+ `found in block ${this.currentBlock.id} `, undefined);
						return await Node.shutdown(55);
					}

					//Enter a postgres transaction block
					if ((result = await this.query(this.beginQuery, [])).error !== undefined) {
						return this.abortProcessing("Failed to load blocks.", true, result.error);
					}

					//Process the transaction
					await this.processTx(transaction, this.currentBlock.id, this.processorAddress!, previousBlockTs, previousBlockHash, false);
					if (Node.txInvalidReason !== undefined || Node.txShouldRetry) {
						//We always roll back when there is an invalid transaction. But only if we should not retry it we increase the invalid counter.
						if ((result = await this.query(this.rollbackQuery, [])).error !== undefined) {
							return this.abortProcessing("Failed to rollback a transaction.", true, result.error);
						}
					}
					if (Node.txShouldRetry) {
						return this.abortProcessing("Failed to process transactions, will retry later.", false);
					}
					if (Node.txInvalidReason !== undefined) {
						//It might have been due to a non-deterministic problem??? (e.g. low memory, timeouts with database connection, etc)
						//Note that the processor never accepts transactions if this happens to it.
						this.transactionFailures++;
						//But if it just keeps happening this isn't the case or requires user attention
						if (this.transactionFailures > 5) {
							await Log.fatal(`Invalid transaction: ${Crypto.binaryToHex(transaction.getId())}, `
								+ `found in block ${this.currentBlock.id}: ${Node.txInvalidReason} `, undefined);
							return await Node.shutdown(55);
						} else {
							return this.abortProcessing(Node.txInvalidReason, true);
						}
					}

					if ((result = await this.query(this.updateProgress, [this.currentBlockId, i + 1])).error !== undefined) {
						return this.abortProcessing("Failed to update progress.", true, result.error);
					}

					//Prepare transaction for being put into the database
					let contractType: string;
					if (Node.txContractHash.equals(Node.createContractHash)) {
						Log.info(`New contract: ${(transaction.getPayloadJson() as CreatePayload).type} `
							+ `(version: ${(transaction.getPayloadJson() as CreatePayload).version}) `);
						contractType = "Create Contract";
					} else if (Node.txContractHash.equals(Node.deleteContractHash)) {
						Log.info(`Contract deleted: ${(transaction.getPayloadJson() as DeletePayload).hash} `);
						contractType = "Delete Contract";
					} else if (this.contractMap.get(Node.txContractHash.toString()) === undefined) {
						Log.warn(`Transaction ${Crypto.binaryToHex(transaction.getId())} was created for unknown contract: ${Crypto.binaryToHex(Node.txContractHash)}`);
						contractType = "Unknown";
					} else {
						contractType = this.contractMap.get(Node.txContractHash.toString())!.type;
					}
					const payload: { [index: string]: any } = transaction.getPayloadJson() === undefined ? {} : transaction.getPayloadJson()!;
					const params: Array<string | number | Buffer> = [
						//Data that makes up the transaction
						transaction.version,
						transaction.getId(),
						transaction.getContractHash(),
						transaction.validTill,
						Crypto.binaryToUtf8(transaction.getPayloadBinary()),
						transaction.getPublicKeyBuffer(),
						transaction.getSignature(),
						//Data about how the transaction has been processed and which block it came from
						Node.txRejectReason === undefined ? TxStatus.Accepted : TxStatus.Rejected,
						this.currentBlock.processedTs,
						this.currentBlock.id,
						i,
						transaction.getAddress(),
						Node.txRejectReason === undefined ? "OK" : Crypto.makeUtf8Postgres(Node.txRejectReason.slice(0, 128)),
						//Additional data to help searching for transactions
						contractType,
						payload.receiver === undefined ? undefined : payload.receiver.toString().slice(0, 35),
						payload.extra1 === undefined ? undefined : payload.extra1.toString().slice(0, 128),
						payload.extra2 === undefined ? undefined : payload.extra2.toString().slice(0, 128)
					];
					if ((result = await this.query(this.insertTx, params)).error !== undefined) {
						if (result.error!.message.indexOf("violates unique constraint") !== -1) {
							//Transactions require a unique id to prevent replay attacks
							await Log.fatal(`Duplicate transaction id: ${Crypto.binaryToHex(transaction.getId())}, `
								+ `found in block ${this.currentBlock.id}.`);
							return await Node.shutdown(55);
						} else if (result.error!.message.indexOf("invalid input syntax for type json") !== -1 ||
							result.error!.message.indexOf("invalid byte sequence for encoding") !== -1) {
							//Transaction may only contain valid characters that can be inserted in the database
							await Log.fatal(`processor accepted transaction: ${Crypto.binaryToHex(transaction.getId())}, `
								+ `found in block ${this.currentBlock.id}, that contains a payload with invalid characters.`);
							return await Node.shutdown(55);
						}
						return this.abortProcessing("Failed to insert transaction.", true, result.error);
					}
					if ((result = await this.query(this.commitQuery, [])).error !== undefined) {
						return this.abortProcessing("Failed to commit transaction.", true, result.error);
					}

					Log.debug(`Processed transaction ${Crypto.binaryToHex(transaction.getId())}, of type: ${contractType}, result: `
						+ (Node.txRejectReason === undefined ? "OK" : Node.txRejectReason));

					//Check if we still have time
					if (Date.now() > processingTime + 15000) {
						outOfTime = true;
						break;
					}
				}

				//If we are out of time do not update current block.
				if (outOfTime) {
					break;
				} else {
					//Mark as continuing with the next block
					if ((result = await this.query(this.updateProgress, [this.currentBlockId + 1, 0])).error !== undefined) {
						return this.abortProcessing("Failed to update progress.", true, result.error);
					}

					//Make ready for the next block
					this.previousBlock = this.currentBlock;
					this.currentBlockId++;
					this.currentTransaction = 0;

					//Check if we still have time
					if (Date.now() > processingTime + 15000) {
						outOfTime = true;
						break;
					}
				}
			}
		}

		//We succeeded, set information for the new block and reset information for mining
		this.transactionFailures = 0;
		this.processingFailures = 0;
		this.justConnected = false;

		if (!this.processedFirstBlock && processedBlocks > 0) {
			Log.info("Succesfully processed first block, everything seems to be working.");
			this.processedFirstBlock = true;
		}

		//Report to the master that we (tried to) process a block
		this.worker.send({ type: "report", memory: process.memoryUsage().heapTotal / 1024 / 1024, processedBlocks });

		//Time to start the next group of blocks, immideately if it ran out of time or processed all blocks.
		setTimeout(() => this.processBlocks(), processedBlocks === 10 || outOfTime ? 0 : Math.min(this.blockInterval! * 1000, 15000));
	}

	/**
	 * Abort processing the current block
	 * @param reason Why do we abort processing
	 * @param rollback Is there a transaction in progress we should rollback?
	 * @param error An optional error.
	 */
	private abortProcessing(reason: string, rollback: boolean, error?: Error): void {
		this.processingFailures++;
		this.shouldRollback = this.shouldRollback || rollback;
		Log.warn(reason, error !== undefined ? new Error(error.message) : undefined);
		//Try again in a moment.
		setTimeout(() => this.processBlocks(), 5000);
	}
}