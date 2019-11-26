import { Node } from "../../node/node";
import { PrivateKey, Transaction, CreatePayload, Crypto, Block, Log, DBBlock, DBTransaction } from "@coinversable/validana-core";
import { Client, types } from "pg";
import { Config } from "../../config";
import { Worker } from "cluster";
import { execSync } from "child_process";

//#region setup

//Settings used for setting up a test database
types.setTypeParser(20, (val: string) => Number.parseInt(val, 10));
types.setTypeParser(1016, (val: string) => val.length === 2 ? [] : val.slice(1, -1).split(",").map((v) => Number.parseInt(v, 10)));
const testdbName = "validana_automatictest_node";
const testUser = "validana_automatictest";
const testPassword = "validana_automatictest";
const postgresPassword = "postgres";

//Helper class for executing tests
class NodeTest extends Node {
	public static errorCounter = -1;
	public static errorCode: string | undefined;

	constructor(worker: Worker, config: Config) {
		super(worker, config);
	}

	/** Same as normal query, but we can force introduce an error. */
	protected query(query: string, params: any[], name?: string): Promise<any> {
		NodeTest.errorCounter--;
		if (NodeTest.errorCounter === 0) {
			throw new Error(query);
		}
		return super.query(query, params, name);
	}

	/** For close database connection. Will be opened again when processBlocks() is called. */
	public static async endConnection(): Promise<void> {
		if (NodeTest.client !== undefined) {
			await NodeTest.client.end();
		}
	}

	/**
	 * When an error occurs it will set a timeout to retry, till then it is assumed to be still processing.
	 * This allows to skip the timeout for tests.
	 */
	public clearTimeout(): void {
		if (this.processTimeout !== undefined) {
			clearTimeout(this.processTimeout);
		}
		this.isProcessingBlocks = false;
	}
}

//Helper functions
/** Create a new block. */
async function insertBlock(trx: Transaction | Transaction[], date = Date.now()): Promise<void> {
	if (previousBlock !== undefined && date <= previousBlock.processedTs) {
		date = previousBlock.processedTs + 1;
	}
	const block = Block.sign({
		previous_block_hash: previousBlock === undefined ? Buffer.alloc(32, 0) : previousBlock.getHash(signPrefix),
		block_id: previousBlock === undefined ? 0 : previousBlock.id + 1,
		processed_ts: date,
		transactions: Transaction.merge(trx instanceof Array ? trx : [trx]),
		version: 1
	}, signPrefix, privateKey);
	previousBlock = block;
	await helperClient.query("INSERT INTO basics.blocks (block_id, version, previous_block_hash, " +
		"processed_ts, transactions, transactions_amount, signature) VALUES ($1, $2, $3, $4, $5, $6, $7);",
		[block.id, block.version, block.getPreviousBlockHash(), block.processedTs, block.getTransactions(),
		block.transactionsAmount, block.getSignature()]);
}
/** Get a transaction by id. */
async function txById(id: Buffer): Promise<DBTransaction & { status: string, contract_type: string }> {
	const transaction = await helperClient.query("SELECT * FROM basics.transactions WHERE transaction_id = $1;", [id]);
	return transaction.rows[0];
}
/** Get the latest available block. */
async function latestBlock(): Promise<DBBlock> {
	const block = await helperClient.query("SELECT * FROM basics.blocks ORDER BY block_id DESC LIMIT 1;", []);
	return block.rows[0];
}

const dummyWorker = { send: () => { } };
const signPrefix = Buffer.from("test");
const privateKey = PrivateKey.generate();
const conf = {
	VNODE_ISPROCESSOR: false,
	VNODE_DBUSER: testUser,
	VNODE_DBPASSWORD: testPassword,
	VNODE_DBNAME: testdbName,
	VNODE_DBPORT: 5432,
	VNODE_DBHOST: "localhost",
	VNODE_SIGNPREFIX: signPrefix.toString(),
	VNODE_PUBLICKEY: privateKey.publicKey.toString("hex")
};
let previousBlock: Block | undefined;
const payload: CreatePayload = {
	type: "bla",
	version: "1.0",
	description: "Does nothing",
	template: {},
	init: "",
	code: Buffer.from("//").toString("base64"),
	validanaVersion: 2
};
const tx = {
	version: 1,
	contract_hash: Buffer.alloc(32),
	valid_till: 0,
	payload: JSON.stringify(payload)
};
let node: NodeTest;
let helperClient: Client;

//#endregion

//Only do integration tests if set
if (process.env.integration === "true" || process.env.INTEGRATION === "true") {
	describe("Node", () => {
		beforeAll(async (done) => {
			try { //Create the test database
				const setupClient = new Client({ user: "postgres", password: postgresPassword, database: "postgres", port: 5432, host: "localhost" });
				await setupClient.connect();
				await setupClient.query(`CREATE DATABASE ${testdbName} WITH ENCODING = 'UTF8';`);
				await setupClient.end();
			} catch (error) { } //In case the database already existed: do nothing
			try { //Fix connection limit and user rights
				execSync(`psql -U postgres -d ${testdbName} -v node_username=${testUser} -v node_password=${testPassword} -v network_username=${testUser} ` +
					`-v network_password=${testPassword} -v backend_username=${testUser} -v backend_password=${testPassword} -f FullSetupDB.sql`,
					{ env: Object.assign({ PGPASSWORD: postgresPassword }, process.env), stdio: "ignore" });
				const setupClient = new Client({ user: "postgres", password: postgresPassword, database: testdbName, port: 5432, host: "localhost" });
				await setupClient.connect();
				await setupClient.query(`ALTER ROLE ${testUser} CONNECTION LIMIT -1;` +
					`GRANT DELETE ON ALL TABLES IN SCHEMA basics TO ${testUser};`);
				await setupClient.end();
			} catch (error) { } //In case setup is done manually: do nothing

			helperClient = new Client({ user: testUser, password: testPassword, database: testdbName, port: 5432, host: "localhost" });
			await helperClient.connect();
			await helperClient.query(
				`DELETE FROM basics.transactions; ` +
				`DELETE FROM basics.blocks; ` +
				`DELETE FROM basics.contracts; ` +
				`DELETE FROM basics.info;`);
			await helperClient.query(`INSERT INTO basics.info (key, value) VALUES ('blockInterval','10'), ('processorNodeVersion', '${process.versions.node}'), ` +
				`('signPrefix', 'test'), ('processorPostgresVersion', current_setting('server_version_num')) ON CONFLICT DO NOTHING;`);
			node = new NodeTest(dummyWorker as any, conf as any);

			//Do not spam console output
			Log.Level = Log.Fatal;

			done();
		});

		afterAll(async (done) => {
			await helperClient.end();
			await NodeTest.endConnection();
			done();
		});

		describe("Setup", () => {
			afterEach(async (done) => {
				node.clearTimeout();
				previousBlock = undefined;
				const resetData =
					`DELETE FROM basics.transactions; ` +
					`DELETE FROM basics.blocks; ` +
					`DELETE FROM basics.contracts; ` +
					`DELETE FROM basics.info WHERE key = 'currentBlock';`;
				await helperClient.query(resetData);
				await NodeTest.endConnection();
				done();
			});

			it("Still processing", async (done) => {
				await insertBlock([]);
				await Promise.all([
					node.processBlocks(),
					node.processBlocks()
				]);
				expect((await latestBlock()).block_id).toBe(0);
				done();
			});
			it("Error load previous block", async (done) => {
				const contractCode = "return 'a89hwf';";
				const id = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id,
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 2
					}))
				}), signPrefix, privateKey));
				await node.processBlocks();
				node.clearTimeout();
				const id2 = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id2,
					contract_hash: Crypto.hash256('"use strict";' + contractCode),
					payload: JSON.stringify({})
				}), signPrefix, privateKey));
				await NodeTest.endConnection();
				NodeTest.errorCounter = 2;
				await node.processBlocks();
				node.clearTimeout();
				expect((await txById(id)).status).toBe("accepted");
				expect<any>(await txById(id2)).toBe(undefined);
				await node.processBlocks();
				expect((await txById(id2)).status).toBe("accepted");
				done();
			});
			it("Error everywhere", async (done) => {
				const contractCode = "await query('SELECT 1;', []); return 'as8ydh9gfn';";
				const id = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id,
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey));
				//There are 14 steps from disconnected to committing a block. So first 13 should fail but recover correctly
				for (let i = 1; i < 13; i++) {
					NodeTest.errorCounter = i;
					await node.processBlocks();
					node.clearTimeout();
					expect<any>(await txById(id)).toBe(undefined);
				}
				//And then it should finally succeed
				await node.processBlocks();
				expect((await txById(id)).status).toBe("accepted");
				done();
			});
			it("Error rollback", async (done) => {
				const contractCode = "return 'a89hwf';";
				const contractHash = Crypto.hash256(contractCode);
				//Create a new contract, which fails to commit
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: Transaction.generateId(),
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey));
				NodeTest.errorCounter = 10;
				await node.processBlocks();
				node.clearTimeout();
				//Now we do a transaction that relies on this contract.
				const id2 = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id2,
					contract_hash: contractHash,
					payload: JSON.stringify({})
				}), signPrefix, privateKey));
				await node.processBlocks();
				expect((await txById(id2)).contract_type).toBe("bla");
				done();
			});
			it("Has previous block", async (done) => {
				expect<any>(await latestBlock()).toBe(undefined);
				await node.processBlocks();
				await insertBlock([]);
				expect((await latestBlock()).block_id).toBe(0);
				await NodeTest.endConnection();
				await insertBlock([]);
				expect((await latestBlock()).block_id).toBe(1);
				done();
			});
		});

		describe("Process Blocks", () => {
			afterEach(async (done) => {
				node.clearTimeout();
				previousBlock = undefined;
				const resetData =
					`DELETE FROM basics.transactions; ` +
					`DELETE FROM basics.blocks; ` +
					`DELETE FROM basics.contracts;` +
					`DELETE FROM basics.info WHERE key = 'currentBlock';`;
				await helperClient.query(resetData);
				await NodeTest.endConnection();

				done();
			});

			it("Simple Tx", async (done) => {
				const contractCode = "return '1';";
				await insertBlock([Transaction.sign(Object.assign({}, tx, {
					transaction_id: Transaction.generateId(),
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey),
				Transaction.sign(Object.assign({}, tx, {
					transaction_id: Transaction.generateId(),
					contract_hash: Crypto.hash256(contractCode),
					payload: JSON.stringify({})
				}), signPrefix, privateKey)]);
				await node.processBlocks();
				expect((await latestBlock()).transactions_amount).toBe(2);
				done();
			});
			it("valid, invalid, valid", async (done) => {
				const contractCode = "return '1'; //aoisdfhj";
				const id1 = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id1,
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey));
				const contractCode2 = "return if";
				const id2 = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id2,
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode2).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey));
				const contractCode3 = "return '1'; //8z9hfrnv";
				const id3 = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id3,
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode3).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey));
				await node.processBlocks();
				expect<any>(await txById(id2)).toBe(undefined);
				done();
			});
			it("delete + unknown", async (done) => {
				const contractCode = "return 'Not ok';";
				const id2 = Transaction.generateId();
				const id3 = Transaction.generateId();
				await insertBlock([Transaction.sign(Object.assign({}, tx, {
					transaction_id: Transaction.generateId(),
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey),
				Transaction.sign(Object.assign({}, tx, {
					transaction_id: id2,
					contract_hash: Buffer.alloc(32, 255),
					payload: JSON.stringify({ hash: Crypto.hash256(contractCode).toString("hex") })
				}), signPrefix, privateKey),
				Transaction.sign(Object.assign({}, tx, {
					transaction_id: id3,
					contract_hash: Crypto.hash256(contractCode),
					payload: JSON.stringify({})
				}), signPrefix, privateKey)]);
				await node.processBlocks();
				expect((await txById(id3)).status).toBe("rejected");
				expect((await txById(id3)).contract_type).toBe("Unknown");
				expect((await latestBlock()).transactions_amount).toBe(3);
				done();
			});
			it("single invalid", async (done) => {
				const contractCode = "return else";
				const id1 = Transaction.generateId();
				await insertBlock(Transaction.sign(Object.assign({}, tx, {
					transaction_id: id1,
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey));
				await node.processBlocks();
				expect<any>(await txById(id1)).toBe(undefined);
				done();
			});
			it("rejected v1", async (done) => {
				const contractCode = "return 'Not ok';";
				const id2 = Transaction.generateId();
				await insertBlock([Transaction.sign(Object.assign({}, tx, {
					transaction_id: Transaction.generateId(),
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 1
					}))
				}), signPrefix, privateKey),
				Transaction.sign(Object.assign({}, tx, {
					transaction_id: id2,
					contract_hash: Crypto.hash256(contractCode),
					payload: JSON.stringify({})
				}), signPrefix, privateKey)]);
				await node.processBlocks();
				expect((await txById(id2)).status).toBe("rejected");
				expect((await latestBlock()).transactions_amount).toBe(2);
				done();
			});
			it("rejected v2", async (done) => {
				const contractCode = "return reject('bla');";
				const id2 = Transaction.generateId();
				await insertBlock([Transaction.sign(Object.assign({}, tx, {
					transaction_id: Transaction.generateId(),
					payload: JSON.stringify(Object.assign({}, payload, {
						code: Buffer.from(contractCode).toString("base64"),
						validanaVersion: 2
					}))
				}), signPrefix, privateKey),
				Transaction.sign(Object.assign({}, tx, {
					transaction_id: id2,
					contract_hash: Crypto.hash256('"use strict";' + contractCode),
					payload: JSON.stringify({})
				}), signPrefix, privateKey)]);
				await node.processBlocks();
				expect((await txById(id2)).status).toBe("rejected");
				expect((await latestBlock()).transactions_amount).toBe(2);
				done();
			});
		});
	});
}