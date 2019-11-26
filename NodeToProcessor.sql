/*
 * Turn a node database into one suitable for the processor.
 * Note: Please check if there are options you want to use or skip, especially if using an existing database cluster.
 * Note: It may fail if the node has not finished processing blocks.
 * Note: It does not revoke permissions from the node user. This should be done manually if desired.
 * Script should be called from psql, where certain variables must be set, for example:
 * psql -U postgres -d blockchain -v processor_username=processor -v processor_password= \
 		-v backend_username=backend -v backend_password= -f NodeToProcessor.sql
 */

/* Check if it finished processing all blocks it downloaded.  */
DO $$ BEGIN
	IF ((SELECT value FROM basics.info WHERE key = 'currentBlock') != ((SELECT block_id FROM basics.blocks ORDER BY block_id DESC LIMIT 1) + 1)::text) THEN
		RAISE EXCEPTION 'Node has not yet finished processing blocks, could not transform database.';
	END IF;
END $$;


/* Big performance improvement. By default this is turned to on in postgres.
Please note that the node assumes it always reads the up to date version of the database.
	If load balancing software cannot garantee this set it to remote_apply instead of off.
Turning this to off means some data reported as stored could be lost after a crash, which doesn't matter
	as we will simply redownload that data from one of the many nodes/the processor. */
--SET synchronous_commit TO off;


/* Alter or remove the needed tables. */
ALTER TABLE basics.transactions ADD COLUMN IF NOT EXISTS create_ts BIGINT;
DROP TABLE basics.info;

/* Create indexes if they do not exist. */
CREATE INDEX IF NOT EXISTS transaction_processed_ts ON basics.transactions (processed_ts);
CREATE INDEX IF NOT EXISTS transaction_status ON basics.transactions (status) WHERE transaction_status = 'new';


/*
 * Add users and their permissions.
 * Postgres does not support IF NOT EXISTS for CREATE ROLE.
 * It also does not support variables inside a DO block.
 */
SET vars.processor_username = :'processor_username';
SET vars.processor_password = :'processor_password';
SET vars.backend_username = :'backend_username';
SET vars.backend_password = :'backend_password';
DO $$ BEGIN
	/* Smart contract can do everything in the public schema. */
	IF NOT EXISTS (SELECT * FROM pg_catalog.pg_roles WHERE rolname = 'smartcontract') THEN
		CREATE ROLE smartcontract;
	END IF;
	/* Smart contract manager can create/delete smart contracts. */
	IF NOT EXISTS (SELECT * FROM pg_catalog.pg_roles WHERE rolname = 'smartcontractmanager') THEN
		CREATE ROLE smartcontractmanager;
	END IF;
	/* The processor user is only to be used by the processor. */
	IF NOT EXISTS (SELECT * FROM pg_catalog.pg_user WHERE usename = current_setting('vars.processor_username')) THEN
		EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L CONNECTION LIMIT 1;',
			current_setting('vars.processor_username'), current_setting('vars.processor_password'));
	END IF;
	EXECUTE format('GRANT CONNECT ON database %I TO %I;', current_database(), current_setting('vars.processor_username'));
	/* The backend user can be used for any backend that wishes to interact with the blockchain in any way. */
	IF NOT EXISTS (SELECT * FROM pg_catalog.pg_user WHERE usename = current_setting('vars.backend_username')) THEN
		EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L;',
			current_setting('vars.backend_username'), current_setting('vars.backend_password'));
	END IF;
	EXECUTE format('GRANT CONNECT ON database %I TO %I;', current_database(), current_setting('vars.backend_username'));
END $$;

/* Give needed user rights to smartcontract and smartcontractmanager. */
GRANT ALL PRIVILEGES ON SCHEMA public TO smartcontract;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO smartcontract;
GRANT SELECT (datname, encoding) ON TABLE pg_catalog.pg_database TO smartcontract;
GRANT SELECT (indexrelid, indkey) ON TABLE pg_catalog.pg_index TO smartcontract;
GRANT SELECT ON TABLE information_schema.tables, information_schema.columns, information_schema.element_types,
	information_schema.key_column_usage, information_schema.referential_constraints, information_schema.table_constraints,
	information_schema.constraint_column_usage, information_schema.constraint_table_usage, information_schema.check_constraints TO smartcontract;
GRANT USAGE ON SCHEMA basics TO smartcontractmanager;
GRANT SELECT, INSERT, DELETE ON TABLE basics.contracts TO smartcontractmanager;

/*
 * Revoke everything they should not have access to (including the common non-deterministic functions).
 * If you need this for other users in your database you can skip this, however make sure not to use them in smart contracts!
 */
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA pg_catalog, information_schema FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION setseed, random, nextval, currval, lastval, now, statement_timestamp, timeofday, transaction_timestamp,
	clock_timestamp, to_timestamp(text, text), to_timestamp(double precision), age(timestamp, timestamp),
	pg_xact_commit_timestamp, pg_last_committed_xact, inet_client_addr, inet_client_port, inet_server_addr,
	inet_server_port, version, set_config, current_setting(text), current_setting(text, boolean), 
	txid_snapshot_in, txid_snapshot_out, txid_snapshot_recv, txid_snapshot_send, txid_current, txid_current_snapshot,
	txid_snapshot_xmin, txid_snapshot_xmax, txid_snapshot_xip, txid_status FROM PUBLIC;

/* Set permissions of the processor. */
GRANT smartcontract, smartcontractmanager TO :processor_username;
GRANT USAGE ON SCHEMA basics TO :processor_username;
GRANT SELECT, UPDATE ON TABLE basics.transactions TO :processor_username;
GRANT SELECT, INSERT ON TABLE basics.blocks TO :processor_username;
GRANT EXECUTE ON FUNCTION current_setting(text) TO :processor_username;

/* Set permissions of the backend. */
GRANT USAGE ON SCHEMA basics, public TO :backend_username;
GRANT SELECT ON ALL TABLES IN SCHEMA basics, public, pg_catalog, information_schema TO :backend_username;
GRANT INSERT (transaction_id, contract_hash, valid_till, payload, public_key, signature, version, create_ts) ON TABLE basics.transactions TO :backend_username;
ALTER DEFAULT PRIVILEGES FOR ROLE :processor_username, smartcontract, smartcontractmanager IN SCHEMA public GRANT SELECT ON TABLES TO :backend_username;
GRANT EXECUTE ON FUNCTION setseed, random, nextval, currval, lastval, now, statement_timestamp, timeofday, transaction_timestamp,
	clock_timestamp, to_timestamp(text, text), to_timestamp(double precision), age(timestamp, timestamp),
	pg_xact_commit_timestamp, pg_last_committed_xact, inet_client_addr, inet_client_port, inet_server_addr,
	inet_server_port, version, set_config, current_setting(text), current_setting(text, boolean), 
	txid_snapshot_in, txid_snapshot_out, txid_snapshot_recv, txid_snapshot_send, txid_current, txid_current_snapshot,
	txid_snapshot_xmin, txid_snapshot_xmax, txid_snapshot_xip, txid_status TO :backend_username;