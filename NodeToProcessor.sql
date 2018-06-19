/* Turn a node database into one suitable for the processor.
Note that it may fail if the node was not finished processing blocks. */

--\c blockchain_node

DO $$
BEGIN
	/* Check if it finished processing all blocks it downloaded. */
	IF ((SELECT value FROM basics.info WHERE key = 'currentBlock') <> ((SELECT block_id FROM basics.blocks ORDER BY block_id DESC LIMIT 1) + 1)::text) THEN
		RAISE EXCEPTION 'Node was not yet finished processing blocks, could not transform database.';
	ELSE
		ALTER TABLE basics.transactions ADD COLUMN IF NOT EXISTS create_ts BIGINT;
		DROP TABLE basics.info;
	END IF;
END $$;

/* We use different names just in case people make the mistake of
starting a node and a processor on the same database, so rename. */
\c postgres
ALTER DATABASE blockchain_node RENAME TO blockchain;
\c blockchain

/* Big performance improvement. By default this is turned to on in postgres.
Please note that the processor assumes it always reads the up to date version of the database.
	If load balancing software cannot garantee this set it to remote_apply instead of off.
Turning this to off means that a backend may report back information, such as a transaction being
	delived to the processor or a transaction being processed that is about to be put into a block, that
	(due to a crash) can still be lost. Note that transactions can be refused by the processor (e.g. due
	to an invalid signature), so turning this on will not guarantee any transaction reported to be
	succesfully delived will actually make it into a block.
For blocks (as given to the nodes) this setting will temporarily be turned back on, so they will
	never be lost through a crash. */
SET synchronous_commit TO OFF;

/* Add users and their permissions. Should be done after all tables are created!
The connection limit is needed to ensure only 1 processor can run at once! */
DO $$
BEGIN
	REVOKE ALL ON SCHEMA public FROM PUBLIC;

	/* The processor user is only to be used by the processor. */
	IF NOT EXISTS (SELECT * FROM pg_catalog.pg_user WHERE usename = 'processor') THEN
		CREATE ROLE processor WITH LOGIN PASSWORD /*'Processor password here'*/ CONNECTION LIMIT 1;
	END IF;

	GRANT CONNECT ON DATABASE blockchain TO processor;
	GRANT USAGE ON SCHEMA basics TO processor;
	GRANT SELECT, UPDATE ON TABLE basics.transactions TO processor;
	GRANT SELECT, INSERT ON TABLE basics.blocks TO processor;
	GRANT SELECT, INSERT, DELETE ON TABLE basics.contracts TO processor;
	GRANT ALL PRIVILEGES ON SCHEMA public TO processor;
	GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO processor;

	/* The backend user can be used for any backend that wishes to interact with the blockchain in any way. */
	IF NOT EXISTS (SELECT * FROM pg_catalog.pg_user WHERE usename = 'backend') THEN
		CREATE ROLE backend WITH LOGIN PASSWORD /*'Backend password here'*/;
	END IF;

	GRANT CONNECT ON DATABASE blockchain TO backend;
	GRANT USAGE ON SCHEMA basics TO backend;
	GRANT INSERT (transaction_id, contract_hash, valid_till, payload, public_key, signature, version, create_ts) ON TABLE basics.transactions TO backend;
	GRANT SELECT ON ALL TABLES IN SCHEMA basics TO backend;
	GRANT USAGE ON SCHEMA public TO backend;
	GRANT SELECT ON ALL TABLES IN SCHEMA public TO backend;
	ALTER DEFAULT PRIVILEGES FOR ROLE processor IN SCHEMA public GRANT SELECT ON TABLES TO backend;
END $$;