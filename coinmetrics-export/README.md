# coinmetrics-export

## Description

`coinmetrics-export` exports blockchain data into formats suitable for analysis by other tools (such as PostgreSQL and Google BigQuery).
The intention is to support multitude of blockchains with a single command-line tool.

## Output

Output formats include:
* textual series of PostgreSQL-compatible INSERT or UPSERT statements
* binary Avro format (suitable for uploading data to Google BigQuery)
* ElasticSearch JSON format (for the ["bulk" API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html))

The smallest exportable unit is a blockchain's block. In case of SQL every source block is exported as a database row. Block's transactions are usually stored in `ARRAY`-typed field of block's row.

For efficiency the tool combines multiple rows into a single SQL statement or Avro block. Number of rows per statement can be adjusted with `--pack-size`.

## Tutorial

Let's say we have full Ethereum node (we tested Geth and Parity) and we want to export Ethereum data continuously into PostgreSQL database for further analysis.

First of all, we need to create necessary tables in PostgreSQL (`coinmetrics-export` doesn't do that automatically). Run the following command:

```bash
coinmetrics-export print-schema --schema ethereum --storage postgres
```

It outputs necessary SQL statements you need to execute manually against your PostgreSQL database.

Then you can start synchronization:

```bash
coinmetrics-export export --blockchain ethereum \
  --continue --threads 16 \
  --output-postgres "host=127.0.0.1 user=postgres"
```

This command will not stop unless something happens (like Ethereum daemon or PostgreSQL server goes down), and will continue synchronizing newly arriving blocks indefinitely. It can be safely interrupted, and on subsequent restart it will query the last synchronized block in database and continue sync from there (the `--continue` option).

In case you need to export a specific range of blocks you can use `--begin-block` (inclusive) and `--end-block` (exclusive) parameters.

Higher number of threads talking to blockchain daemon (`--threads` parameter) usually means increase in synchronization speed, but only up to some limit: test what number on your setup works better. It usually makes sense to make it even higher when connecting to blockchain daemon via network (daemon URL is set with `--api-url` parameter).

No connection pooling for PostgreSQL is performed, new connection is opened and then closed for every write transaction. Usage of external connection pooling middleware such as `pgbouncer` is advised.

Tip: try small block ranges and file output (`--output-postgres-file`) first to see samples of produced SQL statements before messing up with actual PostgreSQL database.

## Options

The tool tries to have sane defaults for most parameters. Note that as the tool performs single pass only, in order to correctly fetch blockchain's main chain we have to keep distance from top block (usually called "rewrite limit") in case blockchain's daemon temporarly picked up wrong chain; this distance is specified as negative value of `--end-block` parameter.

For Ethereum `--trace` option allows to gather transaction traces. Currently only works with Parity synchronized with `--tracing on` option ([Parity docs](https://wiki.parity.io/JSONRPC-trace-module)).

`--ignore-missing-blocks` allows to ignore errors when trying to retrieve blocks from node. Mostly useful with Ripple, as their historical server has many gaps.

By default the tool issues `INSERT` SQL statements and does not overwrite already synchronized records, but this can be changed with `--upsert` flag.

## Exporting IOTA

There is a separate command `export-iota` for exporting IOTA transaction data. Comparing to `export` command, `export-iota` essentially always works in `--continue` mode and there is no way to specify synchronization bounds. Also PostgreSQL database output is required at the moment. Run like this:

```bash
coinmetrics-export export-iota \
	--sync-db iota-sync.db \
	--threads 16 \
	--output-postgres "host=127.0.0.1 user=postgres"
```

The tool can be safely interrupted as it resumes synchronization upon restart. It maintains a small single-file internal helper database (denoted by `--sync-db`) which is not needed to be persisted as it is recreated from scratch on every restart. On start the tool performs special one-time discovery query against PostgreSQL in order to get a list of non-synchronized transactions (i.e. ones referenced by other transactions but not yet existing in database), so it is able to "fill holes" left after interruption.

Normally IOTA node is able to only return transactions happened after latest snapshot. If you have a textual dump file with previous transactions, you can import it using `--read-dump` switch:

```bash
coinmetrics-export export-iota \
	--sync-db iota-sync.db \
	--read-dump \
	--output-postgres "host=127.0.0.1 user=postgres" \
	< transactions.dmp
```
