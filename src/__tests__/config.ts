import { loadConfig } from "../config";
import { PrivateKey, Log } from "@coinversable/validana-core";

describe("Config", () => {
	//Remove all arguments, so it doesn't go looking for a file.
	process.argv.splice(0);

	//Base config with a value for all required keys.
	process.env.VNODE_DBPASSWORD = "test";
	process.env.VNODE_DBPASSWORD_NETWORK = "test2";
	process.env.VNODE_PUBLICKEY = PrivateKey.generate().publicKey.toString("hex");
	process.env.VNODE_PROCESSORHOST = "localhost";

	const config: any = loadConfig();
	for (const key of Object.keys(config)) {
		config[key] = config[key].toString();
	}
	const processEnv = Object.assign({}, process.env, config);

	describe("Environment variables", () => {
		beforeEach(() => {
			process.env = Object.assign({}, processEnv);
			loadConfig();
		});

		it("Load string succesful", () => {
			process.env.VNODE_DBNAME = "348y9hfawjofl";
			expect(loadConfig().VNODE_DBNAME).toEqual("348y9hfawjofl");
		});
		it("Load number succesful", () => expect(() => {
			process.env.VNODE_DBPORT = "1234";
			loadConfig();
		}).not.toThrow());
		it("Load number error", () => expect(() => {
			process.env.VNODE_DBPORT = "awjdif";
			loadConfig();
		}).toThrow());
		it("Load boolean succesful", () => expect(() => {
			process.env.VNODE_ISPROCESSOR = "false";
			loadConfig();
		}).not.toThrow());
		it("Load boolean error", () => expect(() => {
			process.env.VNODE_ISPROCESSOR = "asdfa";
			loadConfig();
		}).toThrow());
		it("Load missing string", () => {
			process.env.VNODE_EXCLUDEREJECTED = undefined;
			expect(() => loadConfig()).not.toThrow();
		});

		it("memory node", () => expect(() => {
			process.env.VNODE_MAXMEMORYNODE = "25";
			loadConfig();
		}).toThrow());
		it("memory network", () => expect(() => {
			process.env.VNODE_MAXMEMORYNETWORK = "25";
			loadConfig();
		}).toThrow());
		it("log level", () => expect(() => {
			process.env.VNODE_LOGLEVEL = (Log.Debug - 1).toString();
			loadConfig();
		}).toThrow());
		it("log level", () => expect(() => {
			process.env.VNODE_LOGLEVEL = (Log.None + 1).toString();
			loadConfig();
		}).toThrow());
		it("db port", () => expect(() => {
			process.env.VNODE_DBPORT = "0";
			loadConfig();
		}).toThrow());

		it("Missing password", () => expect(() => {
			process.env.VNODE_DBPASSWORD = "";
			loadConfig();
		}).toThrow());
		it("Missing public key", () => expect(() => {
			process.env.VNODE_PUBLICKEY = "";
			loadConfig();
		}).toThrow());
		it("Missing processor host", () => expect(() => {
			process.env.VNODE_PROCESSORHOST = "";
			loadConfig();
		}).toThrow());
		it("Missing sign prefix", () => expect(() => {
			process.env.VNODE_ISPROCESSOR = "true";
			process.env.VNODE_TLS = "false";
			loadConfig();
		}).toThrow());
		it("Missing cert while required", () => expect(() => {
			process.env.VNODE_ISPROCESSOR = "true";
			process.env.VNODE_SIGNPREFIX = "test";
			loadConfig();
		}).toThrow());
	});

	describe("Config file", () => {
		//
	});

	describe("Env and config file", () => {
		//
	});
});