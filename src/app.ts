/**
 * @license
 * Copyright Coinversable B.V. All Rights Reserved.
 *
 * Use of this source code is governed by a AGPLv3-style license that can be
 * found in the LICENSE file at https://validana.io/license
 */

import * as Cluster from "cluster";
import { Log, Sandbox } from "@coinversable/validana-core";
import { Node } from "./node/node";
import { Config, loadConfig } from "./config";
import { ProcessorClient } from "./network/processorclient";
import { NodeClient } from "./network/nodeclient";

//What if there is an exception that was not cought
process.on("uncaughtException", (error: Error) => {
	Sandbox.unSandbox();
	if (error.stack === undefined) {
		error.stack = "";
	}
	//Do not accidentially capture the password or encryption key
	if (typeof config !== "undefined") {
		if (config.VNODE_DBPASSWORD !== undefined) {
			error.message = error.message.replace(new RegExp(config.VNODE_DBPASSWORD, "g"), "");
			error.stack = error.stack.replace(new RegExp(config.VNODE_DBPASSWORD, "g"), "");
		}
		if (config.VNODE_SENTRYURL !== undefined) {
			error.message = error.message.replace(new RegExp(config.VNODE_SENTRYURL, "g"), "");
			error.stack = error.stack.replace(new RegExp(config.VNODE_SENTRYURL, "g"), "");
		}
		if (config.VNODE_ENCRYPTIONKEY !== undefined) {
			error.message = error.message.replace(new RegExp(config.VNODE_ENCRYPTIONKEY, "g"), "");
			error.stack = error.stack.replace(new RegExp(config.VNODE_ENCRYPTIONKEY, "g"), "");
		}
	}
	Log.fatal("uncaughtException", error).then(() => process.exit(1));
});
process.on("unhandledRejection", (reason: any, _: Promise<any>) => {
	Sandbox.unSandbox();
	Log.fatal(`unhandledRejection: ${reason}`, new Error("unhandledRejection")).then(() => process.exit(1));
});
//Any process warnings that may be emmited
process.on("warning", (warning: Error) => {
	const shouldSandbox = Sandbox.isSandboxed();
	Sandbox.unSandbox();

	//We only use them while in the sandbox.
	if (warning.message.indexOf("'GLOBAL' is deprecated") === -1 && warning.message.indexOf("'root' is deprecated") === -1) {
		Log.error("Process warning", warning);
	}

	if (shouldSandbox) {
		Sandbox.sandbox();
	}
});

//Load the config
let config: Readonly<Config>;
try {
	config = loadConfig();
	if (config.VNODE_SENTRYURL !== "") {
		Log.setReportErrors(config.VNODE_SENTRYURL);
	}
} catch (error) {
	Log.fatal(`${error.message} Exiting process.`);
	process.exit(1);
}

//Set log information:
Log.options.tags!.type = Cluster.isMaster ? "master" : process.env.worker_type!;
Log.options.tags!.nodeVersion = "1.0.0";
Log.Level = config!.VNODE_LOGLEVEL;

let isShuttingDown: boolean = false;
let isGraceful: boolean = true;

//Start the master or worker.
if (Cluster.isMaster) {
	setupMaster();
} else if (process.env.worker_type === "network") {
	setupNetwork();
} else {
	setupNode();
}

/** Setup the master. The masters only task is to ensure the worker stays online. */
function setupMaster(): void {
	//Everything loaded correctly, notify user the process is running and create the worker.
	Log.info(`Master(pid: ${process.pid}) is running`);
	let networkWorker: Cluster.Worker | undefined;
	let nodeWorker: Cluster.Worker | undefined;
	createWorker("network").then((worker) => networkWorker = worker);
	if (!config.VNODE_ISPROCESSOR) {
		createWorker("node").then((worker) => nodeWorker = worker);
	}

	//If a worker shuts down.
	Cluster.on("exit", (worker: Cluster.Worker, code: number, _: string) => {
		if (code === 0) {
			//Should only happen if master told worker to shut down, for example when we tell the master to shut down.
			Log.info(`Worker ${worker.id}(pid: ${worker.process.pid}) exited.`);
		} else {
			Log.info(`Worker ${worker.id}(pid: ${worker.process.pid}) died with code ${code} `);
			Log.error(`Worker died with code ${code} `);
			if (code >= 50 && code < 60) {
				//So far only db corruption or another instance already running will result in this.
				Log.fatal("Worker signaled it should stay down due to an error it cannot recover from.");
				shutdownMaster(true, code);
			}
		}

		//Restart worker after a 1 second timeout.
		if (!isShuttingDown) {
			if (worker === nodeWorker) {
				nodeWorker = undefined;
				setTimeout(createWorker, 1000, "node", (createdWorker: Cluster.Worker) => nodeWorker = createdWorker);
			} else if (worker === networkWorker) {
				networkWorker = undefined;
				setTimeout(createWorker, 1000, "network", (createdWorker: Cluster.Worker) => networkWorker = createdWorker);
			} else {
				//Exit the program after logging the error
				Log.fatal("Unknown worker exited.", undefined).then(() => process.exit(1));
			}
		}
	});

	let noNetworkNotification: number = 0;
	let noNodeNotification: number = 0;
	Cluster.on("online", (worker) => worker === nodeWorker ? noNodeNotification = 0 : noNetworkNotification = 0);

	//If we receive a message from the node or network process: check memory usage
	Cluster.on("message", (worker, message) => {
		if (worker === nodeWorker) {
			if (typeof message === "object" && message.type === "report" && typeof message.memory === "number") {
				noNodeNotification = 0;
				if (message.memory > config.VNODE_MAXMEMORYNODE) {
					Log.error("Node process using too much memory, restarting.");
					shutdownWorker(nodeWorker.id.toString(), true);
				}
			} else {
				Log.error("Node process send unknown message.");
			}
		} else if (worker === networkWorker) {
			if (typeof message === "object" && message.type === "report" && typeof message.memory === "number") {
				noNetworkNotification = 0;
				if (message.memory > config.VNODE_MAXMEMORYNETWORK) {
					Log.error("Network process using too much memory, restarting.");
					shutdownWorker(networkWorker.id.toString(), true);
				}
			} else {
				Log.error("Network process send unknown message.");
			}
		} else {
			//Exit the program after logging the error
			Log.fatal("Unknown worker reported.", undefined).then(() => process.exit(1));
		}
	});

	//Check if the worker is still responding
	setInterval(() => {
		if (noNodeNotification >= 2 && nodeWorker !== undefined) {
			Log.error("Node process failed to report multiple times, restarting.");
			shutdownWorker(nodeWorker.id.toString(), true);
		}
		if (noNetworkNotification >= 2 && networkWorker !== undefined) {
			Log.error("Network process failed to report multiple times, restarting.");
			shutdownWorker(networkWorker.id.toString(), true);
		}
		noNodeNotification++;
		noNetworkNotification++;
	}, 30000);

	//What to do if we receive a signal to shutdown
	process.on("SIGINT", () => shutdownMaster(false));
	process.on("SIGTERM", () => shutdownMaster(true));
}

/** Shutdown the master. */
function shutdownMaster(hardkill: boolean, code: number = 0): void {
	if (!isShuttingDown) {
		Log.info("Master shutting down...");

		isShuttingDown = true;

		//Send shutdown signal to all workers.
		isGraceful = true;
		for (const id of Object.keys(Cluster.workers)) {
			shutdownWorker(id, hardkill);
		}

		setInterval(() => {
			if (Object.keys(Cluster.workers).length === 0) {
				Log.info("Shutdown completed");
				process.exit(code === 0 && !isGraceful ? 1 : code);
			}
		}, 500);
	}
}

/** Setup the network worker. This worker is responsible for downloading new blocks. */
function setupNetwork(): void {
	//If this process encounters an error when being created/destroyed. We do not do a graceful shutdown in this case.
	Cluster.worker.on("error", (error) => {
		Log.error("Network worker encountered an error", error);

		process.exit(1);
	});

	Log.info(`Network worker ${Cluster.worker.id} (pid: ${process.pid}) started`);

	const network = config.VNODE_ISPROCESSOR ? new ProcessorClient(Cluster.worker, config) : new NodeClient(Cluster.worker, config);

	//If the master sends a shutdown message we do a graceful shutdown.
	Cluster.worker.on("message", (message: string) => {
		Log.info(`Network worker ${process.pid} received message: ${message} `);
		if (message === "shutdown" && !isShuttingDown) {
			//The node will also end the process after it is done.
			isShuttingDown = true;
			network.shutdown();
		}
	});

	//What to do if we receive a signal to shutdown?
	process.on("SIGTERM", () => {
		Log.info(`Network worker ${process.pid} received SIGTERM`);
		if (!isShuttingDown) {
			isShuttingDown = true;
			network.shutdown();
		}
	});
	process.on("SIGINT", () => {
		Log.info(`Network worker ${process.pid} received SIGINT`);
		if (!isShuttingDown) {
			isShuttingDown = true;
			network.shutdown();
		}
	});
}

/** Setup the node worker. This worker is responsible for validating and processing blocks and transactions. */
function setupNode(): void {
	//If this process encounters an error when being created/destroyed. We do not do a graceful shutdown in this case.
	Cluster.worker.on("error", (error) => {
		Log.error("Node worker encountered an error", error);

		process.exit(1);
	});

	Log.info(`Node worker ${Cluster.worker.id} (pid: ${process.pid}) started`);

	const node = new Node(Cluster.worker, config);
	node.processBlocks();

	//If the master sends a shutdown message we do a graceful shutdown.
	Cluster.worker.on("message", (message: string) => {
		Log.info(`Node worker ${process.pid} received message: ${message} `);
		if (message === "shutdown" && !isShuttingDown) {
			//The node will also end the process after it is done.
			isShuttingDown = true;
			Node.shutdown();
		}
	});

	//What to do if we receive a signal to shutdown?
	process.on("SIGTERM", () => {
		Log.info(`Node worker ${process.pid} received SIGTERM`);
		if (!isShuttingDown) {
			isShuttingDown = true;
			Node.shutdown();
		}
	});
	process.on("SIGINT", () => {
		Log.info(`Node worker ${process.pid} received SIGINT`);
		if (!isShuttingDown) {
			isShuttingDown = true;
			Node.shutdown();
		}
	});
}

/**
 * Create a new worker. Will retry until it succeeds.
 * @param type: The type of worker to create, either the network worker or the node worker.
 */
async function createWorker(type: "network" | "node", timeout: number = 5000): Promise<Cluster.Worker> {
	try {
		//For internal environment variables use snake_case
		return await new Promise<Cluster.Worker>((resolve) => {
			resolve(Cluster.fork(Object.assign(config, { worker_type: type })));
		});
	} catch (error) {
		if (timeout >= 60000) {
			//Problem seems to not resolve itsself.
			Log.error("Failed to start the worker many times in a row.", error);
		} else {
			Log.warn("Failed to start worker", error);
		}
		//Increase retry time up to 5 min max.
		return new Promise<Cluster.Worker>((resolve) => {
			setTimeout(() => {
				createWorker(type, Math.min(timeout * 1.5, 300000)).then(() => resolve());
			}, timeout);
		});
	}
}

/**
 * Shutdown a worker.
 * @param id the id of the worker to shut down.
 * @param hardkill whether to kill the worker if it does not gracefully shutdown within 10 seconds.
 */
function shutdownWorker(id: string, hardkill: boolean): void {
	//Send shutdown message for a chance to do a graceful shutdown.
	if (Cluster.workers[id] !== undefined) {
		Cluster.workers[id]!.send("shutdown", undefined, (error: Error | null) => {
			//Doesn't matter if it fails, there will be a hard kill in 10 seconds.
			//(write EPIPE errors mean the worker closed the connection, properly because it already exited.)
			if (error !== null && error.message !== "write EPIPE") {
				Log.warn(`Worker ${id} shutdown failed`, error);
			}
		});
	} else {
		Log.info(`Trying to shutdown non-existing worker ${id} `);
		Log.error("Trying to shutdown non-existing worker");
	}

	if (hardkill) {
		//Give every handler 10 seconds to shut down before doing a hard kill.
		setTimeout(() => {
			if (Cluster.workers[id] !== undefined) {
				isGraceful = false;
				Log.info(`Worker ${id} not shutting down.`);
				Log.fatal("Hard killing worker, is there a contract with an infinite loop somewhere?");
				process.kill(Cluster.workers[id]!.process.pid, "SIGKILL");
			}
		}, 10000);
	}
}