[![Travis CI Build Status](https://travis-ci.org/coinmetrics-io/haskell-tools.svg?branch=master)](https://travis-ci.org/coinmetrics-io/haskell-tools) [![Docker Repository on Quay](https://quay.io/repository/quyse/coinmetrics-haskell-tools/status "Docker Repository on Quay")](https://quay.io/repository/quyse/coinmetrics-haskell-tools)

# Haskell-based CoinMetrics.io tools

These tools are used by CoinMetrics.io team for exporting data from blockchains into analytical databases,
allowing to perform SQL queries, for instance, generating aggregated information on Ethereum tokens.

## Status

The project is in early alpha deep development seems-like-its-working-oh-crap state. Command line interface is more or less stable but may change. Please use with caution.

Supported cryptocurrencies:

* [Bitcoin](https://bitcoin.org/) (WIP)
* [Ethereum](https://www.ethereum.org/)
* [Cardano](https://www.cardanohub.org/)
* [IOTA](https://iota.org/)
* [Monero](https://getmonero.org/)
* [NEM](https://nem.io/)
* [NEO](https://neo.org/)
* [Ripple](https://ripple.com/)
* [Stellar](https://www.stellar.org/)

## Prebuilt Binaries (experimental)

There's no stable releases yet. All binaries are built on Travis CI.

The easiest way to run the tools is to use docker.

Pull the latest version:
```bash
docker pull quay.io/quyse/coinmetrics-haskell-tools
```

Run `coinmetrics-export` tool:
```bash
docker run -it --rm --net host quay.io/quyse/coinmetrics-haskell-tools coinmetrics-export <arguments>
```

Alternatively you can download binaries from Bintray:

* [generic Linux](https://bintray.com/coinmetrics/haskell-tools)
* [.deb packages](https://bintray.com/coinmetrics/haskell-tools-deb)
* [.rpm packages](https://bintray.com/coinmetrics/haskell-tools-rpm)

## Packages

* `coinmetrics` - library with some base primitives for exporting blockchain data into different formats.
* `coinmetrics-bitcoin` - library specific to Bitcoin.
* `coinmetrics-cardano` - library specific to Cardano.
* `coinmetrics-ethereum` - library specific to Ethereum.
* `coinmetrics-export` - utility for exporting data from blockchains in formats suitable for inserting into analytics databases (SQL, Avro).
* `coinmetrics-iota` - library specific to IOTA.
* `coinmetrics-monero` - library specific to Monero.
* `coinmetrics-nem` - library specific to NEM.
* `coinmetrics-neo` - library specific to NEO.
* `coinmetrics-ripple` - library specific to Ripple.
* `coinmetrics-stellar` - library specific to Stellar.

## Building

Get [stack](https://docs.haskellstack.org/en/stable/install_and_upgrade/).

Run `stack build --copy-bins --local-bin-path <path for executables>`. Executables will be placed by specified path.

The code is only tested on Linux (but maybe works on other OSes too).

## Using coinmetrics-export

`coinmetrics-export` exports blockchain data into formats suitable for analysis by other tools (such as PostgreSQL and Google BigQuery).
The intention is to support multitude of blockchains with a single command-line tool.

Proper documentation is yet to be written. Please run `coinmetrics-export --help` for list of commands, and `coinmetrics-export <command> --help` for info on specific command.

## Output

Output formats include:
* textual series of PostgreSQL-compatible INSERT or UPSERT statements
* binary Avro format (suitable for uploading data to Google BigQuery)

Note that PostgreSQL output is the most tested format and is considered primary. Some features even require PostgreSQL output (such as `export --continue` and `export-iota` commands).

The smallest exportable unit is a blockchain's block. In case of SQL every source block is exported as a database row. Block's transactions are usually stored in `ARRAY`-typed field of block's row.

For efficiency the tool combines multiple rows into a single SQL statement or Avro block. Number of rows can be adjusted with `--pack-size`.

## Tutorial

Let's say we have full Ethereum node (we tested Geth and Parity) and we want to export Ethereum data continuously into PostgreSQL database for further analysis.

First of all, we need to create necessary tables in PostgreSQL (`coinmetrics-export` doesn't do that automatically). Run the following command:

```bash
coinmetrics-export print-schema --schema ethereum --storage postgres
```

It outputs necessary SQL statements you need to execute manually on your PostgreSQL database.

Then you can start synchronization right away:

```bash
coinmetrics-export export --blockchain ethereum \
  --continue --threads 16 \
  --output-postgres "host=127.0.0.1 user=postgres"
```

This command will not stop unless something happens (like Ethereum daemon or PostgreSQL server goes down), and will continue synchronizing newly arriving blocks indefinitely. It can be safely interrupted, and on subsequent restart it will query the last synchronized block in database and continue sync from there (the `--continue` option).

In case you need to export a manually selected range of blocks you can use `--begin-block` (inclusive) and `--end-block` (exclusive) parameters.

Higher number of threads talking to blockchain daemon (`--threads` parameter) usually means increase in synchronization speed, but only up to some limit: test what number on your setup works better. It usually makes sense to make it even higher when connecting to blockchain daemon via network (daemon URL is set with `--api-url` parameter).

No connection pooling for PostgreSQL is performed, new connection is opened and then closed for every write transaction. Usage of external connection pooling middleware such as `pgbouncer` is advised.

Tip: try small block ranges and file output (`--output-postgres-file`) first to see samples of produced SQL statements before messing up with actual PostgreSQL database.

## Special options

For Ethereum `--trace` option allows to gather transaction traces. Currently only works with Parity synchronized with `--tracing on` option ([Parity docs](https://wiki.parity.io/JSONRPC-trace-module)).

`--ignore-missing-blocks` allows to ignore errors when trying to retrieve blocks from node. Mostly useful with Ripple, as their historical server has many gaps.

## Blockchain export defaults

The tool tries to have sane defaults for most parameters. Note that as the tool performs single pass only, in order to correctly fetch blockchain's main chain we have to keep distance from top block (usually called "rewrite limit") in case blockchain's daemon temporarly picked up wrong chain; this distance is specified as negative value of `--end-block` parameter. The exception is Ripple and Stellar, because for them we use history data instead of live node data, and it's never rewritten.

| `--blockchain` | `--api-url`                                              | `--begin-block` | `--end-block` |
|----------------|----------------------------------------------------------|-----------------|---------------|
| `bitcoin`      | `http://127.0.0.1:8332/`                                 | `0`             | `-1000`       |
| `cardano`      | `http://127.0.0.1:8100/`                                 | `2`             | `-1000`       |
| `ethereum`     | `http://127.0.0.1:8545/`                                 | `0`             | `-1000`       |
| `monero`       | `http://127.0.0.1:18081/json_rpc`                        | `0`             | `-60`         |
| `nem`          | `http://127.0.0.1:7890/`                                 | `1`             | `-360` ([rewrite limit](https://nemproject.github.io/#initiating-transactions)) |
| `neo`          | `http://127.0.0.1:10332/`                                | `0`             | `-1000`       |
| `ripple`       | `https://data.ripple.com/`                               | `32570` ([genesis ledger](https://ripple.com/build/data-api-v2/#genesis-ledger)) | `0` (history data, no rewrites) |
| `stellar`      | `http://history.stellar.org/prd/core-live/core_live_001` | `1`             | `0` (history data, no rewrites) |

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
