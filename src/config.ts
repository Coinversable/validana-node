/**
 * @license
 * Copyright Coinversable B.V. All Rights Reserved.
 *
 * Use of this source code is governed by a AGPLv3-style license that can be
 * found in the LICENSE file at https://validana.io/license
 */

import * as Cluster from "cluster";
import * as FS from "fs";
import * as Path from "path";
import { Log, Crypto, PublicKey } from "validana-core";

/** The config for the node. Using all capitalized names because this is the standard for environment variables. */
export interface Config extends StringConfig, NumberConfig, BooleanConfig { }
export interface StringConfig {
	VNODE_DBUSER: string; //Database user
	VNODE_DBPASSWORD: string; //Password of database user
	VNODE_DBNAME: string; //Database name
	VNODE_DBHOST: string; //Database host
	VNODE_SENTRYURL: string; //Used to automatically inform developers about execution errors. Care is taken that no sensitive information is reported.
	VNODE_ENCRYPTIONKEY: string; //Hex representation of the key used to encrypt data between clients

	//In case this is not a processor node
	VNODE_PROCESSORHOST: string; //The processor address to start retrieving peers and basic information about the blockchain.

	//In case this is a processor node:
	VNODE_PUBLICKEY: string; //The public key of the processor
	VNODE_KEYPATH: string; //Certificate (in case you use no reverse proxy)
	VNODE_CERTPATH: string; //Certificate (in case you use no reverse proxy)
	VNODE_NODEVERSION: string; //Should be the same as the processor
	VNODE_SIGNPREFIX: string; //Should be the same as the processor
}
export interface NumberConfig {
	VNODE_LOGLEVEL: number; //The Debug level we use.
	VNODE_MAXMEMORYNODE: number; //How much memory is it allowed to use before we force a restart
	VNODE_MAXMEMORYNETWORK: number; //How much memory is it allowed to use before we force a restart
	VNODE_DBPORT: number; //Database port to use.
	VNODE_LISTENPORT: number; //which port we use for listening to incomming connections.
	VNODE_PEERTIMEOUT: number; //How long to wait before retrying a peer after a connection problem (in seconds)
	VNODE_CACHEDBLOCKS: number; //Number of blocks we cache in memory for spreading to peers faster.
	VNODE_MAXSENDSIZE: number; //Maximum size of messages we send to other peers (in bytes)
	VNODE_MAXSENDAMOUNT: number; //Maximum amount of blocks we will send to other peers (in bytes)
	VNODE_MAXPEERS: number; //Maximum number of peers we want to be connected to

	VNODE_PROCESSORPORT: number; //Which port to the processor is listening to or should listen on

	//In case this is not the processor node
	VNODE_MAXOUTSTANDINGLBOCKS: number; //Maximum amount of outstanding block downloads before waiting till they are finished and stored in the DB
	VNODE_MINDOWNLOADSPEED: number; //Mimimum download speed we want from other peers before dropping them (in kB/s)
	VNODE_MAXREQUESTAMOUNT: number; //Maximum amount of blocks we request other peers to send to us
	VNODE_MAXRECEIVESIZE: number; //Maximum size of message we will allow other peers to send to us (in bytes)
	VNODE_MINPEERS: number; //Mimumum number of peers we want to maintain (if possible)

	//In case this is a processor node:
	VNODE_REMEMBERPEER: number; //How long it should remember peers after having connected (in hours)
	VNODE_LATESTEXISTINGBLOCK: number; //Normally retrieved from the database, but if you are certain there are more (but they are not in the database) use this.
	VNODE_BLOCKINTERVAL: number; //Should be the same as the processor
	VNODE_MAXBLOCKSIZE: number; //Should be the same as the processor
}
export interface BooleanConfig {
	VNODE_ISPROCESSOR: boolean; //Whether or not this is the processor node.
	VNODE_TLS: boolean; //Whether to use tls or not
}

//The default values
const stringConfig: StringConfig = {
	VNODE_DBUSER: "node",
	VNODE_DBNAME: "blockchain_node",
	VNODE_DBHOST: "localhost",
	VNODE_DBPASSWORD: "",
	VNODE_SENTRYURL: "",
	VNODE_ENCRYPTIONKEY: "",

	VNODE_PROCESSORHOST: "",
	VNODE_PUBLICKEY: "",

	VNODE_KEYPATH: "",
	VNODE_CERTPATH: "",
	VNODE_NODEVERSION: "10",
	VNODE_SIGNPREFIX: ""
};
const numberConfig: NumberConfig = {
	VNODE_LOGLEVEL: Log.Debug,
	VNODE_MAXMEMORYNODE: 512,
	VNODE_MAXMEMORYNETWORK: 512,
	VNODE_DBPORT: 5432,
	VNODE_LISTENPORT: 0,
	VNODE_MAXSENDSIZE: 10000000,
	VNODE_MAXRECEIVESIZE: 25000000,
	VNODE_MAXSENDAMOUNT: 500,
	VNODE_MAXREQUESTAMOUNT: 500,
	VNODE_MINDOWNLOADSPEED: 10,
	VNODE_MINPEERS: 5,
	VNODE_MAXPEERS: 20,
	VNODE_PEERTIMEOUT: 900,
	VNODE_MAXOUTSTANDINGLBOCKS: 5000,
	VNODE_CACHEDBLOCKS: 1000,

	VNODE_PROCESSORPORT: 443,

	VNODE_REMEMBERPEER: 24,
	VNODE_LATESTEXISTINGBLOCK: -1,
	VNODE_BLOCKINTERVAL: 5,
	VNODE_MAXBLOCKSIZE: 1000000
};
const booleanConfig: BooleanConfig = {
	VNODE_ISPROCESSOR: false,
	VNODE_TLS: true
};

/** Load the configuration values from the environment variables and config file. */
export function loadConfig(): Readonly<Config> {
	loadEnv();
	if (Cluster.isMaster) {
		loadFile();
		validate();
	}

	return Object.assign(stringConfig, numberConfig, booleanConfig);
}

/** Load all keys from environment variables. */
function loadEnv(): void {
	for (const key of Object.keys(stringConfig)) {
		const processKey = process.env[key];
		if (processKey !== undefined) {
			stringConfig[key as keyof StringConfig] = processKey;
		}
	}
	for (const key of Object.keys(numberConfig)) {
		const processKey = process.env[key];
		if (processKey !== undefined) {
			const envValue = Number.parseInt(processKey);
			if (!Number.isSafeInteger(envValue)) {
				throw new Error(`Invalid value: ${envValue} for environment variable: ${key}, expected an integer.`);
			} else {
				numberConfig[key as keyof NumberConfig] = envValue;
			}
		}
	}
	for (const key of Object.keys(booleanConfig)) {
		const processKey = process.env[key];
		if (processKey !== undefined) {
			if (processKey !== "true" && processKey !== "false") {
				throw new Error(`Invalid value: ${processKey} for environment variable: ${key}, expected 'true' or 'false'.`);
			} else {
				booleanConfig[key as keyof BooleanConfig] = processKey === "true";
			}
		}
	}
}

/** Load all keys from the config file. */
function loadFile(): void {
	//arg 0 is node.exe, arg 1 is this script.js, arg2+ are the passed arguments
	if (process.argv.length >= 3) {
		//Determine where the config file should be and if it exists.
		const configPath = Path.resolve(process.argv[process.argv.length - 1]);
		if (!FS.existsSync(configPath)) {
			throw new Error(`Unable to find file: ${configPath}.`);
		}
		//Load config file.
		let configFile: any;
		try {
			configFile = JSON.parse(Crypto.binaryToUtf8(FS.readFileSync(configPath)));
		} catch (error) {
			throw new Error(`Unable to load config file: ${configPath}: ${(error as Error).message}: ${(error as Error).stack}.`);
		}

		//Load all values from the config file
		for (const key of Object.keys(configFile)) {
			if (stringConfig.hasOwnProperty(key)) {
				if (typeof configFile[key] !== "string") {
					throw new Error(`Invalid type in config file for key: ${key}, expected a string.`);
				} else {
					stringConfig[key as keyof StringConfig] = configFile[key].toString();
				}
			} else if (numberConfig.hasOwnProperty(key)) {
				if (!Number.isSafeInteger(configFile[key])) {
					throw new Error(`Invalid type in config file for key: ${key}, expected an integer.`);
				} else {
					numberConfig[key as keyof NumberConfig] = configFile[key];
				}
			} else if (booleanConfig.hasOwnProperty(key)) {
				if (typeof configFile[key] !== "boolean") {
					throw new Error(`Invalid type in config file for key: ${key}, expected a boolean.`);
				} else {
					booleanConfig[key as keyof BooleanConfig] = configFile[key];
				}
			} else {
				Log.warn(`Unknown config file key: ${key}`);
			}
		}
	}
}

/** Validate if all values are correct. */
function validate(): void {
	//Check if we are running at least node js version 7.6, as is needed for the node to function.
	const version: number[] = [];
	for (const subVersion of process.versions.node.split(".")) {
		version.push(Number.parseInt(subVersion, 10));
	}
	if (!(version[0] > 7 || version[0] === 7 && version[1] >= 6)) {
		throw new Error(`Node requires at least node js version 7.6 to function, while currently running: ${process.versions.node}.`);
	}
	//Check (if we use encryption) if the encryption key is correct.
	if (stringConfig.VNODE_ENCRYPTIONKEY !== "" &&
		(stringConfig.VNODE_ENCRYPTIONKEY.length !== 64 || !Crypto.isHex(stringConfig.VNODE_ENCRYPTIONKEY))) {

		throw new Error("Invalid encryption key, if an encryption key is used it should be an hex encoded length 64 string.");
	}
	//Check if the database password is given
	if (stringConfig.VNODE_DBPASSWORD === "") {
		throw new Error(`No database password provided.`);
	}
	//Check if log level is correct
	if (numberConfig.VNODE_LOGLEVEL < Log.Debug || numberConfig.VNODE_LOGLEVEL > Log.None) {
		throw new Error(`Invalid log level: ${numberConfig.VNODE_LOGLEVEL}, should be 0-5.`);
	}
	//Check if all ports have valid values
	if (numberConfig.VNODE_DBPORT <= 0 || numberConfig.VNODE_DBPORT > 65535) {
		throw new Error(`Invalid db port: ${numberConfig.VNODE_DBPORT}, should be 1-65535.`);
	}
	if (numberConfig.VNODE_PROCESSORPORT <= 0 || numberConfig.VNODE_PROCESSORPORT > 65535) {
		throw new Error(`Invalid processor port: ${numberConfig.VNODE_PROCESSORPORT}, should be 1-65535.`);
	}
	if (numberConfig.VNODE_LISTENPORT < 0 || numberConfig.VNODE_LISTENPORT > 65535) {
		throw new Error(`Invalid listen port: ${numberConfig.VNODE_LISTENPORT}, should be 0-65535.`);
	}
	//Check if all numberConfig have reasonable values:
	if (numberConfig.VNODE_MAXMEMORYNETWORK < 128) {
		throw new Error(`Invalid max memory: ${numberConfig.VNODE_MAXMEMORYNETWORK}, should be at least 128 MB.`);
	}
	if (numberConfig.VNODE_MAXSENDAMOUNT <= 0) {
		throw new Error(`Invalid number for max send amount (${numberConfig.VNODE_MAXSENDAMOUNT}) `
			+ `or max send size (${numberConfig.VNODE_MAXSENDSIZE}), should be at least 1`);
	}
	if (numberConfig.VNODE_LATESTEXISTINGBLOCK < -1) {
		throw new Error(`Invalid latest existing block (${numberConfig.VNODE_LATESTEXISTINGBLOCK}), should be at least -1`);
	}
	if (numberConfig.VNODE_PEERTIMEOUT < 30) {
		throw new Error(`Invalid peer timeout: ${numberConfig.VNODE_PEERTIMEOUT}, should be at least 30.`);
	}
	if (numberConfig.VNODE_CACHEDBLOCKS < 100) {
		throw new Error(`Invalid cached blocks: ${numberConfig.VNODE_CACHEDBLOCKS}, should be at least 100.`);
	}

	if (booleanConfig.VNODE_ISPROCESSOR) {
		//In case this is the processor node check if additional values are correct
		if (stringConfig.VNODE_SIGNPREFIX === "") {
			throw new Error("No sign prefix given, which is required for the processor node.");
		}
		if (numberConfig.VNODE_BLOCKINTERVAL <= 0) {
			throw new Error(`Invalid block interval: ${numberConfig.VNODE_BLOCKINTERVAL}, should be at least 1 second.`);
		}
		if (numberConfig.VNODE_REMEMBERPEER < 0) {
			throw new Error(`Invalid remember peer time: ${numberConfig.VNODE_BLOCKINTERVAL}, should be at least 0 hours.`);
		}
		if (numberConfig.VNODE_MAXBLOCKSIZE < 110000) {
			throw new Error(`Invalid max block size: ${numberConfig.VNODE_MAXBLOCKSIZE}, should be at least 110000 bytes.`);
		}
		if (numberConfig.VNODE_MAXPEERS <= 0) {
			throw new Error(`Invalid max peers: ${numberConfig.VNODE_MAXPEERS}, should be at least 1.`);
		}
		if (booleanConfig.VNODE_TLS && (stringConfig.VNODE_KEYPATH === "" || stringConfig.VNODE_CERTPATH === "")) {
			throw new Error("Invalid keypath or certpath, using tls but one of them is undefined.");
		}
		//If we use tls check if we can load key and certificate
		if (booleanConfig.VNODE_TLS) {
			stringConfig.VNODE_KEYPATH = Path.resolve(stringConfig.VNODE_KEYPATH);
			stringConfig.VNODE_CERTPATH = Path.resolve(stringConfig.VNODE_CERTPATH);
			if (!FS.existsSync(stringConfig.VNODE_CERTPATH)) {
				throw new Error(`Invalid keypath: Unable to find file ${stringConfig.VNODE_KEYPATH}`);
			}
			if (!FS.existsSync(stringConfig.VNODE_CERTPATH)) {
				throw new Error(`Invalid keypath: Unable to find file ${stringConfig.VNODE_CERTPATH}`);
			}
		}
	} else {
		//If it is not the processor node check if the processor host has been set and a valid number for minpeers has been set.
		if (stringConfig.VNODE_PROCESSORHOST === "") {
			throw new Error("Processor host is missing.");
		}
		if (numberConfig.VNODE_MINPEERS <= 0 || numberConfig.VNODE_MAXREQUESTAMOUNT <= 0 || numberConfig.VNODE_MINDOWNLOADSPEED <= 0) {
			throw new Error(`Invalid min peers (${numberConfig.VNODE_MINPEERS}), max request amount (${numberConfig.VNODE_MAXREQUESTAMOUNT}), `
				+ `min download speed (${numberConfig.VNODE_MINDOWNLOADSPEED}) or max receive size (${numberConfig.VNODE_MAXRECEIVESIZE}), should be at least 1.`);
		}
		if (numberConfig.VNODE_MAXPEERS < numberConfig.VNODE_MINPEERS) {
			throw new Error(`Invalid max peers: ${numberConfig.VNODE_MAXPEERS}, should be at least MINPEERS (${numberConfig.VNODE_MINPEERS}).`);
		}
		if (numberConfig.VNODE_MAXMEMORYNODE < 128) {
			throw new Error(`Invalid max memory: ${numberConfig.VNODE_MAXMEMORYNODE}, should be at least 128 MB.`);
		}
		if (!Crypto.isHex(stringConfig.VNODE_PUBLICKEY) || !PublicKey.isValidPublic(Crypto.hexToBinary(stringConfig.VNODE_PUBLICKEY))) {
			throw new Error("Invalid or missing public key, should be hex encoded length 66 public key.");
		}
		if (numberConfig.VNODE_MAXOUTSTANDINGLBOCKS < 100 || numberConfig.VNODE_MAXOUTSTANDINGLBOCKS < numberConfig.VNODE_MAXREQUESTAMOUNT) {
			throw new Error(`Invalid max outstanding blocks: ${numberConfig.VNODE_MAXOUTSTANDINGLBOCKS}, should be at least 100 `
				+ `and at least MAXREQUESTAMOUNT (${numberConfig.VNODE_MAXREQUESTAMOUNT}).`);
		}
	}
}