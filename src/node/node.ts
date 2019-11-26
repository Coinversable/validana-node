/*!
 * @license
 * Copyright Coinversable B.V. All Rights Reserved.
 *
 * Use of this source code is governed by a AGPLv3-style license that can be
 * found in the LICENSE file at https://validana.io/license
 */

import * as Cluster from "cluster";
import { Client } from "pg";
import { Crypto, Log, Basic, TxStatus, DBBlock, Block, Transaction, CreatePayload, DeletePayload, PublicKey } from "@coinversable/validana-core";
import { Config } from "../config";

/** The node is responsible for validating all blocks and transactions as well as building the current state depending on the transactions. */
export class Node extends Basic {

	//Queries for various actions
	private readonly versionQuery = "SELECT current_setting('server_version_num');";
	private readonly getInfo = "SELECT * FROM basics.info;";
	private readonly beginBlockQuery = "BEGIN; SET LOCAL ROLE smartcontract; SAVEPOINT transaction;";
	private readonly rollbackSavepointQuery = "ROLLBACK TO SAVEPOINT transaction;";
	private readonly newSavepointQuery = "RELEASE SAVEPOINT transaction; SAVEPOINT transaction;";
	private readonly updateProgress = "INSERT INTO basics.info (key, value) VALUES ('currentBlock', $1) "
		+ "ON CONFLICT ON CONSTRAINT info_pkey DO UPDATE SET value = EXCLUDED.value;";
	private readonly getNextBlocks = "SELECT * FROM basics.blocks WHERE block_id >= $1 ORDER BY block_id ASC LIMIT $2;";
	private readonly getPreviousBlock = "SELECT * FROM basics.blocks WHERE block_id = $1;";
	private readonly insertTxs = "INSERT INTO basics.transactions (version, transaction_id, contract_hash, valid_till, payload, " +
		"public_key, signature, status, processed_ts, block_id, position_in_block, sender, message, contract_type, receiver) " +
		"VALUES (unnest($1::SMALLINT[]), unnest($2::BYTEA[]), unnest($3::BYTEA[]), unnest($4::BIGINT[]), unnest($5::JSON[]), " +
		"unnest($6::BYTEA[]), unnest($7::BYTEA[]), unnest($8::basics.transaction_status[]), $9, $10, generate_series(0, $11-1), " +
		"unnest($12::VARCHAR(35)[]), unnest($13::VARCHAR(128)[]), unnest($14::VARCHAR(64)[]), unnest($15::VARCHAR(35)[]));";
	private readonly getLatestExistingBlock = "SELECT block_id FROM basics.blocks ORDER BY block_id DESC LIMIT 1;";

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

	private readonly worker: Cluster.Worker;
	private latestExistingBlock: number = -1;
	protected isProcessingBlocks: boolean = false;
	protected notificationsClient: Client;
	protected processTimeout: NodeJS.Timeout | undefined;

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
		}, undefined, (init) => this.worker.send({ type: "init", init }));

		this.worker = worker;
		this.processorAddress = new PublicKey(Crypto.hexToBinary(config.VNODE_PUBLICKEY)).getAddress();

		this.notificationsClient = new Client({
			user: config.VNODE_DBUSER_NETWORK,
			database: config.VNODE_DBNAME,
			password: config.VNODE_DBPASSWORD,
			port: config.VNODE_DBPORT,
			host: config.VNODE_DBHOST
		}).on("error", (error) => {
			//Do not accidentally capture password
			error.message = error.message.replace(new RegExp(config.VNODE_DBPASSWORD, "g"), "");
			Log.warn("Problem with database connection.", error);
		}).on("end", () => {
			setTimeout(() => this.listen(), 5000);
		}).on("notification", (notification) => {
			this.latestExistingBlock = Number.parseInt(notification.payload!, 10);
			this.processBlocks();
		});

		this.listen();

		//Setup heartbeat
		setInterval(() => {
			const memory = process.memoryUsage();
			this.worker.send({ type: "report", memory: (memory.heapTotal + memory.external) / 1024 / 1024 });
		}, 15000);
	}

	private async listen(): Promise<void> {
		try {
			await this.notificationsClient.connect();
		} catch (error) {
			//End will be called, which will create a new connection in a moment.
		}
		try {
			await this.notificationsClient.query("LISTEN downloaded;");
		} catch (error) {
			//End will be called, which will create a new connection in a moment.
			await this.notificationsClient.end().catch(() => { });
		}
	}

	/** Takes up to 10 unprocessed blocks and processes them. If there are more block available it will continue right away. */
	public async processBlocks(): Promise<void> {
		if (this.isProcessingBlocks) {
			return;
		}
		this.isProcessingBlocks = true;

		//If we had multiple failures in a row the problem seems to not resolve itsself. We will still try to process again...
		if (this.processingFailures > 10) {
			Log.error("Node failed to process blocks multiple times in a row.");
		}

		//Connect to the DB (if it isn't connected already).
		this.justConnected = await this.connect() || this.justConnected;

		//Rollback old in progress transactions (won't emit an error if there is nothing to roll back!)
		if (this.justConnected || this.shouldRollback) {
			try {
				await this.query("ROLLBACK;", []);
			} catch (error) {
				return this.abortProcessing("Failed to rollback transactions after reconnecting.", true, error);
			}

			try { //Get all smart contracts (again) as we may have just rolled back a contract we added/deleted.
				await this.loadSmartContracts();
			} catch (error) {
				return this.abortProcessing("Failed to retrieve smart contracts.", false, error);
			}

			this.shouldRollback = false;
		}

		//Retrieve information required for this blockchain
		if (this.justConnected) {

			//Get all settings from the database
			let settings: Array<{ key: string, value: string }>;
			try {
				settings = (await this.query(this.getInfo, [])).rows;
			} catch (error) {
				return this.abortProcessing("Failed to retrieve info transactions.", false, error);
			}
			if (settings.length === 0) {
				return this.abortProcessing("Still haven't retrieved info from processor.", false);
			}
			//Fill in settings from the database.
			this.currentBlockId = 0;
			for (const row of settings) {
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
					default:
						Log.warn(`Unknown database key: ${row.key}`);
				}
			}
			//Either everything or nothing should be filled in at this point.
			if (this.blockInterval === undefined || this.processorNodeVersion === undefined ||
				this.signPrefix === undefined || this.processorPostgresVersion === undefined) {

				return this.abortProcessing("One or more keys not set after loading info from db.", false);
			}

			//Get our current progress from the database
			if (this.currentBlockId !== 0) {
				let previousBlock: DBBlock | undefined;
				try {
					previousBlock = (await this.query(this.getPreviousBlock, [this.currentBlockId - 1])).rows[0];
				} catch (error) {
					return this.abortProcessing("Failed to retrieve previous block.", false, error);
				}
				if (previousBlock === undefined) {
					return await Node.shutdown(54, `Block ${this.currentBlockId - 1} not found in the database while marked as already processed.`);
				}
				//This should never fail, because we already processed this block before.
				this.previousBlock = new Block(previousBlock);
			} else {
				this.previousBlock = undefined;
			}

			//Get the total amount of existing blocks from the database
			try {
				const latestBlock = (await this.query(this.getLatestExistingBlock, [])).rows[0];
				this.latestExistingBlock = latestBlock === undefined ? -1 : latestBlock.block_id;
			} catch (error) {
				return this.abortProcessing("Failed to determine amount of existing blocks.", false, error);
			}

			try { //Set statement timeout so queries (mainly from smart contracts) don't take forever.
				await this.query(`SET statement_timeout = ${this.blockInterval * 1000};`, []);
			} catch (error) {
				return this.abortProcessing("Failed to set statement_timeout.", false, error);
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
					Log.warn(`Different node js version, running version: ${process.versions.node}, version used by processor: ` +
						`${this.processorNodeVersion!}. Choosing to ignore these differences may lead to errors later on.`);
				}
			}
			//It should be compatible but we still log it just in case.
			Log.options.tags.requiredNodeVersion = this.processorNodeVersion!;

			try { //Check if we are running the required postgres version
				const result = (await this.query(this.versionQuery, [])).rows[0];
				const ourPostgresVersion = Number.parseInt(result.current_setting, 10);
				if (Number.isNaN(ourPostgresVersion) || ourPostgresVersion < 90500) {
					return await Node.shutdown(52, "Too old or invalid postgres version, requires at least 9.5, shutting down.");
				} else {
					//Up to postgres 10 a 0.1 version difference means a breaking change.
					if ((ourPostgresVersion < 100000 && Math.abs(ourPostgresVersion - this.processorPostgresVersion!) >= 10)
						|| (ourPostgresVersion >= 100000 && Math.abs(ourPostgresVersion - this.processorPostgresVersion!) >= 1000)) {

						const requiredVersion = Math.floor(this.processorPostgresVersion! / 10000) + "." + Math.floor((this.processorPostgresVersion! % 1000) / 100)
							+ "." + (this.processorPostgresVersion! % 10);
						const ourVersion = Math.floor(ourPostgresVersion / 10000) + "." + Math.floor((ourPostgresVersion % 1000) / 100)
							+ "." + (ourPostgresVersion % 10);
						Log.warn(`Different Postgres version, running version: ${ourVersion}, version used by processor: ` +
							`${requiredVersion}. Choosing to ignore these differences may lead to errors later on.`);
					}

					//It should be compatible but we still log it just in case.
					Log.options.tags.postgresVersion = result.current_setting;
					Log.options.tags.requiredPostgresVersion = "" + this.processorPostgresVersion!;
					this.verifiedVersion = true;
				}
			} catch (error) {
				return this.abortProcessing("Failed to verify postgres version.", false, error);
			}
		}

		//Retrieve and validate the blocks
		let dbBlocks: DBBlock[];
		try { //Select 10 blocks at once (if possible)
			dbBlocks = (await this.query(this.getNextBlocks, [this.currentBlockId, 10], "getBlocks")).rows;
		} catch (error) {
			return this.abortProcessing("Failed to load blocks.", false, error);
		}
		for (const block of dbBlocks) {
			try {
				this.currentBlock = new Block(block);
			} catch (error) {
				return await Node.shutdown(55, `Failed to construct block ${this.currentBlockId} from database block.`, error);
			}

			//No need to validate block signature again, the network part will not accept blocks that are not from the processor.
			//if (!this.currentBlock.verifySignature(this.signPrefix!, this.processorPublicKey!)) {}

			if (!this.currentBlock.verifyWithPreviousBlock(this.signPrefix!, this.previousBlock)) {
				//If the previous block hash does not match or the processed time goes backwards
				return await Node.shutdown(55, `Processor created invalid block ${this.currentBlock.id}, timestamp or `
					+ `previous block hash do not match the previous block.`);
			} else if (this.currentBlock.processedTs > Date.now() + 300000 && !this.notifiedGreaterTime) {
				//The block has been created at least 5 min in the future compared to this node
				//This shouldn't happen, however 'current time' is non-deterministic and there is no absolute truth as to what time it is
				this.notifiedGreaterTime = true;
				Log.error(`Processor created block ${this.currentBlock.id} that it claims has been processed in the future. ` +
					`Our time: ${Date.now()}, block processed time: ${this.currentBlock.processedTs}`);
			} else if (this.previousBlock !== undefined && this.currentBlock.processedTs > this.previousBlock.processedTs + this.blockInterval! * 2000 + 5000) {
				//The block was created with a significant time difference compared to the previous node.
				//Note that this is not really an error and can be faked by the processor, its just a help for the nodes to see what is going on
				Log.warn(`Processor created block ${this.currentBlock.id} with a significant later timestamp then the previous block. ` +
					`Previous block timestamp: ${new Date(this.previousBlock.processedTs).toISOString()}, ` +
					`this block timestamp: ${new Date(this.currentBlock.processedTs).toISOString()}`);
			}

			let transactionsToProcess: Transaction[];
			try {
				transactionsToProcess = Transaction.unmerge(this.currentBlock.getTransactions());
			} catch (error) {
				return await Node.shutdown(55, `Failed to contruct transactions found in block ${this.currentBlock.id}.`, error);
			}

			try { //Process the transaction
				await this.query(this.beginBlockQuery, []);
			} catch (error) {
				return this.abortProcessing("Failed to begin a transaction.", true, error);
			}

			const previousBlockTs = this.previousBlock?.processedTs ?? 0;
			const previousBlockHash = this.previousBlock?.getHash(this.signPrefix!) ?? Buffer.alloc(32, 0);
			const processedTxs: Array<[Transaction, TxStatus, string, string]> = [];
			for (const transaction of transactionsToProcess) {
				//We check the signature seperately for better error messages.
				if (!transaction.verifySignature(this.signPrefix!)) {
					return await Node.shutdown(55, `Invalid signature for transaction: ${Crypto.binaryToHex(transaction.getId())}, `
						+ `found in block ${this.currentBlock.id}`);
				}

				const processResult = await this.processTx(transaction, this.currentBlock.id, this.currentBlock.processedTs,
					this.processorAddress!, previousBlockTs, previousBlockHash, false);

				try { //Update transaction savepoint
					if (processResult.status !== "accepted" && processResult.status !== "v1Rejected") {
						await this.query(this.rollbackSavepointQuery, []);
					} else {
						await this.query(this.newSavepointQuery, []);
					}
				} catch (error) {
					return this.abortProcessing("Failed to rollback a transaction.", true, error);
				}

				if (processResult.status === "retry") {
					//All transactions must be processed in order, so we can't continue with other transactions
					return this.abortProcessing("Failed to process transactions, will retry later.", false);
				}

				if (processResult.status === "invalid") {
					//It might have been due to a non-deterministic problem??? (e.g. low memory, timeouts with database connection, etc)
					//Note that the processor never accepts transactions if this happens to it.
					this.transactionFailures++;
					//But if it just keeps happening this isn't the case or requires user attention
					if (this.transactionFailures > 3) {
						return await Node.shutdown(55, `Invalid transaction: ${Crypto.binaryToHex(transaction.getId())}, `
							+ `found in block ${this.currentBlock.id}: ${processResult.message}`);
					} else {
						return this.abortProcessing(processResult.message, true);
					}
				}

				//Prepare transaction for being put into the database
				let contractType: string;
				if (Node.txContractHash.equals(Node.createContractHash)) {
					Log.info(`New contract: ${(transaction.getPayloadJson() as CreatePayload).type} `
						+ `(version: ${(transaction.getPayloadJson() as CreatePayload).version})`);
					contractType = "Create Contract";
				} else if (Node.txContractHash.equals(Node.deleteContractHash)) {
					Log.info(`Contract deleted: ${(transaction.getPayloadJson() as DeletePayload).hash}`);
					contractType = "Delete Contract";
				} else if (this.contractMap.get(Node.txContractHash.toString()) === undefined) {
					Log.warn(`Transaction ${Crypto.binaryToHex(transaction.getId())} was created for unknown contract: ${Crypto.binaryToHex(Node.txContractHash)}`);
					contractType = "Unknown";
				} else {
					contractType = this.contractMap.get(Node.txContractHash.toString())!.type;
					Log.debug(`Processed transaction ${Crypto.binaryToHex(transaction.getId())}, of type: ${contractType}, result: ${processResult.message}`);
				}
				processedTxs.push([
					transaction,
					processResult.status === "accepted" ? TxStatus.Accepted : TxStatus.Rejected,
					Crypto.makeUtf8Postgres(processResult.message.slice(0, 128)),
					contractType
				]);
			}

			try {
				await this.query("RESET ROLE;", []);
			} catch (error) {
				return this.abortProcessing("Failed to reset role.", true, error);
			}

			//Finish processing all transactions
			const versions: number[] = [];
			const ids: Buffer[] = [];
			const contractHashes: Buffer[] = [];
			const validTills: number[] = [];
			const payloads: string[] = [];
			const publicKeys: Buffer[] = [];
			const signatures: Buffer[] = [];
			const statuses: TxStatus[] = [];
			const senders: string[] = [];
			const messages: string[] = [];
			const contractTypes: string[] = [];
			const receivers: Array<string | undefined> = [];
			for (const processedTxRow of processedTxs) {
				const processedTx = processedTxRow[0];
				versions.push(processedTx.version);
				ids.push(processedTx.getId());
				contractHashes.push(processedTx.getContractHash());
				validTills.push(processedTx.validTill);
				payloads.push(processedTx.getPayloadBinary().toString());
				publicKeys.push(processedTx.getPublicKeyBuffer());
				signatures.push(processedTx.getSignature());
				statuses.push(processedTxRow[1]);
				senders.push(processedTx.getAddress());
				messages.push(processedTxRow[2]);
				contractTypes.push(processedTxRow[3]);

				const payload = processedTx.getPayloadJson();
				const receiver = (payload as any)?.receiver;
				// tslint:disable-next-line: no-null-keyword
				receivers.push(receiver == null ? undefined : String(receiver).slice(0, 35));
			}
			const params: any[] = [versions, ids, contractHashes, validTills, payloads, publicKeys,
				signatures, statuses, this.currentBlock.processedTs, this.currentBlockId,
				this.currentBlock.transactionsAmount, senders, messages, contractTypes, receivers];

			try { //Insert the transaction. Depending on the error we try again or consider the processor to have manipulated the data.
				await this.query(this.insertTxs, params);
			} catch (error) {
				if (error.code === "23505") {
					//Transactions require a unique id to prevent replay attacks
					return await Node.shutdown(55, `Duplicate transaction id found in block: ${this.currentBlockId}.`, error);
				} else if (error.code === "22P02" || error.code === "22P03") {
					//Transaction may only contain valid characters that can be inserted in the database
					return await Node.shutdown(55, `Processor accepted transaction found in block ${this.currentBlock.id}` +
						`, that contains a payload with invalid characters.`, error);
				}
				return this.abortProcessing("Failed to insert transaction.", true, error);
			}

			try { //Mark as continuing with the next block
				await this.query(this.updateProgress, [this.currentBlockId + 1]);
			} catch (error) {
				return this.abortProcessing("Failed to update progress.", true, error);
			}

			if (!Node.isShuttingDown) {
				try {
					await this.query("COMMIT;", []);
				} catch (error) {
					return this.abortProcessing("Failed to commit transaction.", true, error);
				}
			}

			try { //Notify listeners that a new block has been processed. We want creating blocks to succeed even if this fails.
				await this.query(`NOTIFY blocks, '${JSON.stringify({
					block: this.currentBlockId, ts: this.currentBlock.processedTs, txs: this.currentBlock.transactionsAmount, other: 0
				})}';`, []);
			} catch (error) {
				Log.warn("Failed to notify listeners of new block.", error);
			}

			//Make ready for the next block
			this.previousBlock = this.currentBlock;
			this.currentBlockId++;
		}

		//We succeeded, set information for the new block and reset information for mining
		this.transactionFailures = 0;
		this.processingFailures = 0;
		this.justConnected = false;

		if (!this.processedFirstBlock && dbBlocks.length > 0) {
			Log.info("Succesfully processed first block, everything seems to be working.");
			this.processedFirstBlock = true;
		}

		this.isProcessingBlocks = false;
		//If there are more blocks continue processing right away.
		if (this.latestExistingBlock > this.currentBlockId) {
			this.processTimeout = setTimeout(() => this.processBlocks(), 0);
		}
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
		this.processTimeout = setTimeout(() => {
			this.isProcessingBlocks = false;
			this.processBlocks();
		}, 5000);
	}
}