# Haskell-based CoinMetrics.io tools

These tools are used by CoinMetrics.io team for exporting data from blockchains into analytical databases,
allowing to perform SQL queries, for instance, generating aggregated information on Ethereum tokens.

## Packages

* `coinmetrics` - library with some base primitives for exporting blockchain data into different formats.
* `coinmetrics-ethereum` - library specific to Ethereum.
* `coinmetrics-export` - utility for exporting data from blockchains in formats suitable for inserting into analytics databases (SQL, Avro).

## Building

Get [stack](https://docs.haskellstack.org/en/stable/install_and_upgrade/).

Run `stack build --copy-bins --local-bin-path <path for executables>`. Executables will be placed by specified path.

The code is only tested on Linux (but probably work on other OSes too).

## Using coinmetrics-export

`coinmetrics-export` exports blockchain data into formats suitable for analysis by other tools (such as PostgreSQL and Google BigQuery).
So far it supports Ethereum only, but the intention is to support multitude of blockchains.
Output formats include SQL (PostgreSQL-compatible) and Avro (used successfully for uploading data to Google BigQuery).

Proper documentation is yet to be written. Please run `coinmetrics-export [<command>] --help` for actual and more detailed info.

### Usage examples

Export desired range of blocks (from 1000000 to 1999999, as end block is exclusive), using 16 threads, and output data simultaneously to SQL file and Avro file:
```bash
coinmetrics-export export \
  --begin-block 1000000 --end-block 2000000 \
  --threads 16 \
  --output-postgres-file data.sql \
  --output-avro-file data.avro
```
Fetching data with multiple threads (`--threads` parameter) allows to compensate for latency, especially if talking to blockchain daemon via network.

Continuously export blocks, starting from the beginning and never stopping (negative value for `--end-block` means sync continuously as new blocks arrive; the value means how much distance we want to keep from top block), and output straight into PostgreSQL database specified by connection string:
```bash
coinmetrics-export export \
  --begin-block 0 --end-block -1000 \
  --threads 16 \
  --output-postgres "host=127.0.0.1 user=postgres"
```

Get SQL commands for initializing PostgreSQL database:
```bash
coinmetrics-export print-schema --schema ethereum --storage postgres
```
