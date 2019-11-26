import { PrivateKey, Transaction, Block, Crypto, Log, DBBlock } from "@coinversable/validana-core";
import { Client, types, QueryResult } from "pg";
import { ProcessorClient } from "../network/processorclient";
import { NodeClient } from "../network/nodeclient";
import { execSync } from "child_process";

//#region setup

//Settings used for setting up a test database
const testdbName = "validana_automatictest_node";
const testdbName2 = "validana_automatictest_node2";
const testUser = "validana_automatictest";
const testPassword = "validana_automatictest";
const postgresPassword = "postgres";

//Helper classes
class PCTest extends ProcessorClient {
	public clearCache(): void {
		this.latestRetrievedBlock = 0;
		this.cachedBlocksStartId = -1;
		this.cachedBlocks.splice(0);
		this.latestExistingBlock = 0;
	}
	public getCachedBlocks(): Block[] {
		return this.cachedBlocks;
	}
	public clearInterval(): void {
		clearInterval(this.latestExistingBlockInterval!);
	}
}
class NCTest extends NodeClient {
	private static toReset: NCTest[] = [];
	constructor(worker: any, config: any) {
		super(worker, config);
		NCTest.toReset.push(this);
	}
	public static async reset(): Promise<void> {
		const promises = [];
		for (const nc of NCTest.toReset) {
			if (nc.latestExistingBlockInterval !== undefined) {
				clearInterval(nc.latestExistingBlockInterval);
			}
			for (const peer of nc.peers) {
				promises.push(peer.disconnect());
			}
			nc.pool.end();
			nc.shutdownServer();
		}
		NCTest.toReset = [];
		await Promise.all(promises);
	}
	public async reset(): Promise<void> {
		if (this.latestExistingBlockInterval !== undefined) {
			clearInterval(this.latestExistingBlockInterval);
		}
		const promises: any[] = [];
		for (const peer of this.peers) {
			promises.push(peer.disconnect());
		}
		this.pool.end();
		this.shutdownServer();
		NCTest.toReset.splice(NCTest.toReset.indexOf(this), 1);
		await Promise.all(promises);
	}
	protected async query(query: string, values?: any[]): Promise<QueryResult> {
		try {
			return await this.pool.query(query, values);
		} catch (e) {
			//So we can use multiple nodes with the same database
			if (e.message.indexOf("value violates unique constraint")) {
				return {} as any;
			}
			throw e;
		}
	}
}

//Helper functions
async function insertBlock(txs: Transaction | Transaction[], wait = false, date = Date.now()): Promise<void> {
	const block = Block.sign({
		previous_block_hash: previousBlock === undefined ? Buffer.alloc(32, 0) : previousBlock.getHash(signPrefix),
		block_id: previousBlock === undefined ? 0 : previousBlock.id + 1,
		processed_ts: date,
		transactions: Transaction.merge(txs instanceof Array ? txs : [txs]),
		version: 1
	}, signPrefix, privateKey);
	previousBlock = block;
	//Insert block
	await helperClient.query("INSERT INTO basics.blocks (block_id, version, previous_block_hash, " +
		"processed_ts, transactions, transactions_amount, signature) VALUES ($1, $2, $3, $4, $5, $6, $7);",
		[block.id, block.version, block.getPreviousBlockHash(), block.processedTs, block.getTransactions(),
		block.transactionsAmount, block.getSignature()]);
	//Notify all that there is a new block
	await helperClient.query(`NOTIFY blocks, '${JSON.stringify({
		block: block.id, ts: block.processedTs, txs: block.transactionsAmount, other: 0
	})}';`, []);
	//Wait a moment for the blocks to sync between nodes.
	if (wait) {
		await new Promise((resolve) => setTimeout(resolve, 250));
	}
}
async function latestBlock(): Promise<DBBlock> {
	const block = await helperClient2.query("SELECT * FROM basics.blocks ORDER BY block_id DESC LIMIT 1;", []);
	return block.rows[0];
}

types.setTypeParser(20, (val: string) => Number.parseInt(val, 10));
types.setTypeParser(1016, (val: string) => val.length === 2 ? [] : val.slice(1, -1).split(",").map((v) => Number.parseInt(v, 10)));
const dummyWorker = { send: () => { } };
const signPrefix = Buffer.from("test");
const privateKey = PrivateKey.generate();
const configProc = {
	VNODE_ISPROCESSOR: false,
	VNODE_DBUSER: testUser,
	VNODE_DBPASSWORD: testPassword,
	VNODE_DBNAME: testdbName,
	VNODE_DBPORT: 5432,
	VNODE_DBHOST: "localhost",
	VNODE_SIGNPREFIX: signPrefix.toString(),
	VNODE_PUBLICKEY: privateKey.publicKey.toString("hex"),
	VNODE_LATESTEXISTINGBLOCK: -1,
	VNODE_ENCRYPTIONKEY: "",
	VNODE_MAXMEMORYNETWORK: 512,
	VNODE_DBMINCONNECTIONS: 0,
	VNODE_DBMAXCONNECTIONS: 10,
	VNODE_MAXOUTSTANDINGBLOCKS: 1500,
	VNODE_LISTENPORT: 0,
	VNODE_BLOCKINTERVAL: 1,
	VNODE_MINDOWNLOADSPEED: 10,
	VNODE_MINPEERS: 2,
	VNODE_MAXPEERS: 3,
	VNODE_PEERTIMEOUT: 60,
	VNODE_TLS: false,
	VNODE_DBUSER_NETWORK: testUser,
	VNODE_DBPASSWORD_NETWORK: testPassword,
	VNODE_PROCESSORHOST: "localhost",
	VNODE_PROCESSORPORT: 49472,
	VNODE_MAXBLOCKSIZE: 1000000,
	VNODE_REMEMBERPEER: 100
};
const configNode = Object.assign({}, configProc, { VNODE_DBNAME: testdbName2 });
let helperClient = new Client({ user: "postgres", password: postgresPassword, database: testdbName, port: 5432, host: "localhost" });
let helperClient2 = new Client({ user: "postgres", password: postgresPassword, database: testdbName2, port: 5432, host: "localhost" });
let previousBlock: Block | undefined;
let pc: PCTest;

//#endregion

//Only do integration tests if set
if (process.env.integration === "true" || process.env.INTEGRATION === "true") {
	describe("Node", () => {
		beforeAll(async (done) => {
			for (const dbName of [testdbName, testdbName2]) {
				try { //Create the test database
					const setupClient = new Client({ user: "postgres", password: postgresPassword, database: "postgres", port: 5432, host: "localhost" });
					await setupClient.connect();
					await setupClient.query(`CREATE DATABASE ${dbName} WITH ENCODING = 'UTF8';`);
					await setupClient.end();
				} catch (error) { } //In case the database already existed: do nothing
				try { //Fix connection limit and user rights
					execSync(`psql -U postgres -d ${dbName} -v node_username=${testUser} -v node_password=${testPassword} -v network_username=${testUser} ` +
						`-v network_password=${testPassword} -v backend_username=${testUser} -v backend_password=${testPassword} -f FullSetupDB.sql`,
						{ env: Object.assign({ PGPASSWORD: postgresPassword }, process.env), stdio: "ignore" });
					const setupClient = new Client({ user: "postgres", password: postgresPassword, database: testdbName, port: 5432, host: "localhost" });
					await setupClient.connect();
					await setupClient.query(`ALTER ROLE ${testUser} CONNECTION LIMIT -1;` +
						`GRANT DELETE ON ALL TABLES IN SCHEMA basics TO ${testUser};`);
					await setupClient.end();
				} catch (error) { } //In case setup is done manually: do nothing
			}

			//Setup starting values
			const resetData =
				`DELETE FROM basics.transactions; ` +
				`DELETE FROM basics.blocks; ` +
				`DELETE FROM basics.contracts;` +
				`DELETE FROM basics.info WHERE key = 'currentBlock';`;
			helperClient = new Client({ user: "postgres", password: postgresPassword, database: testdbName, port: 5432, host: "localhost" });
			await helperClient.connect();
			await helperClient.query(resetData);
			await helperClient.query(`INSERT INTO basics.info (key, value) VALUES ('blockInterval','10'), ('processorNodeVersion', '${process.versions.node}'), ` +
				` ('signPrefix', 'test'), ('processorPostgresVersion', current_setting('server_version_num')) ON CONFLICT DO NOTHING;`);
			helperClient2 = new Client({ user: "postgres", password: postgresPassword, database: testdbName2, port: 5432, host: "localhost" });
			await helperClient2.connect();
			await helperClient2.query(resetData);
			await helperClient2.query(`INSERT INTO basics.info (key, value) VALUES ('blockInterval','10'), ('processorNodeVersion', '${process.versions.node}'), ` +
				` ('signPrefix', 'test'), ('processorPostgresVersion', current_setting('server_version_num')) ON CONFLICT DO NOTHING;`);

			pc = new PCTest(dummyWorker as any, configProc as any);
			await new Promise((resolve) => setTimeout(resolve, 500)); //Give a moment to setup

			//Do not spam console output
			Log.Level = Log.Fatal;

			done();
		});

		afterAll(async (done) => {
			await pc.shutdownInfoServer();
			await pc.shutdownServer();
			await pc.clearInterval();
			done();
		});

		describe("Setup", () => {
			afterEach(async (done) => {
				previousBlock = undefined;
				//Reset any data if needed
				const resetData =
					`DELETE FROM basics.transactions; ` +
					`DELETE FROM basics.blocks; ` +
					`DELETE FROM basics.contracts;`;
				await helperClient.query(resetData);
				await helperClient2.query(resetData);
				await NCTest.reset();
				if (pc !== undefined) {
					pc.clearCache();
				}
				done();
			});

			it("empty block", async (done) => {
				new NCTest(dummyWorker, configNode);
				await insertBlock([], true);
				expect<any>(await latestBlock()).not.toBe(undefined);
				done();
			});

			it("new block", async (done) => {
				new NCTest(dummyWorker, configNode);
				await insertBlock([], true);
				expect<any>(await latestBlock()).not.toBe(undefined);
				await insertBlock([], true);
				expect((await latestBlock()).block_id).toBe(1);
				done();
			});

			it("Encryption key", async (done) => {
				const key = Crypto.binaryToHex(Crypto.sha256(Math.random().toString()));
				const configProcSpecial = Object.assign({}, configProc, { VNODE_PROCESSORPORT: 49473, VNODE_ENCRYPTIONKEY: key });
				const configNodeSpecial = Object.assign({}, configNode, { VNODE_PROCESSORPORT: 49473, VNODE_ENCRYPTIONKEY: key });
				const pcSpecial = new PCTest(dummyWorker as any, configProcSpecial as any);
				new NCTest(dummyWorker, configNodeSpecial);
				await insertBlock([], true);
				expect<any>(await latestBlock()).not.toBe(undefined);
				pcSpecial.shutdownInfoServer();
				pcSpecial.shutdownServer();
				pcSpecial!.clearInterval();
				done();
			});

			it("cache", async (done) => {
				new NCTest(dummyWorker, configNode);
				await Promise.all([
					insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]),
					insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([], true)
				]);
				expect(pc.getCachedBlocks().length === 10);
				expect((await latestBlock()).block_id).toBe(11);
				done();
			});

			it("multiple peers", async (done) => {
				const nc = new NCTest(dummyWorker, configNode);
				new NCTest(dummyWorker, configNode); //Alleen deze laten inserten, de rest niet
				new NCTest(dummyWorker, configNode);
				new NCTest(dummyWorker, configNode);
				new NCTest(dummyWorker, configNode);
				const nc2 = new NCTest(dummyWorker, configNode);
				await Promise.all([insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([]), insertBlock([], true)]);
				expect(pc.getCachedBlocks().length === 1);
				expect((await latestBlock()).block_id).toBe(6);
				await nc.reset();
				await nc2.reset();
				await insertBlock([], true);
				expect((await latestBlock()).block_id).toBe(7);
				done();
			});
		});
	});
}