[![Travis CI Build Status](https://travis-ci.org/coinmetrics-io/haskell-tools.svg?branch=master)](https://travis-ci.org/coinmetrics-io/haskell-tools) [![Docker Repository on Quay](https://quay.io/repository/quyse/coinmetrics-haskell-tools/status "Docker Repository on Quay")](https://quay.io/repository/quyse/coinmetrics-haskell-tools)

# Haskell-based CoinMetrics.io tools

These tools are used by CoinMetrics.io team for exporting data from blockchains into analytical databases,
allowing to perform SQL queries, for instance, generating aggregated information on Ethereum tokens.

## Status

The project is in very early stage.

Supported cryptocurrencies:

* [Ethereum](https://www.ethereum.org/)
* [Cardano](https://www.cardanohub.org/)
* [IOTA](https://iota.org/) (WIP)
* [NEM](https://nem.io/) (WIP)
* [NEO](https://neo.org/) (WIP)
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
* `coinmetrics-cardano` - library specific to Cardano.
* `coinmetrics-ethereum` - library specific to Ethereum.
* `coinmetrics-export` - utility for exporting data from blockchains in formats suitable for inserting into analytics databases (SQL, Avro).
* `coinmetrics-iota` - library specific to IOTA.
* `coinmetrics-nem` - library specific to NEM.
* `coinmetrics-neo` - library specific to NEO.
* `coinmetrics-ripple` - library specific to Ripple.
* `coinmetrics-stellar` - library specific to Stellar.

## Building

Get [stack](https://docs.haskellstack.org/en/stable/install_and_upgrade/).

Run `stack build --copy-bins --local-bin-path <path for executables>`. Executables will be placed by specified path.

The code is only tested on Linux (but probably work on other OSes too).

## Using coinmetrics-export

`coinmetrics-export` exports blockchain data into formats suitable for analysis by other tools (such as PostgreSQL and Google BigQuery).
The intention is to support multitude of blockchains with a single command-line tool.
Output formats include SQL (PostgreSQL-compatible) and Avro (used successfully for uploading data to Google BigQuery).

Proper documentation is yet to be written. Please run `coinmetrics-export --help` for list of commands, and `coinmetrics-export <command> --help` for info on specific command.

## Tutorial

Let's say we have full Ethereum node (we tested Geth and Parity) and we want to export Ethereum data continuously into PostgreSQL database for further analysis.

First of all, we need to create necessary tables in PostgreSQL (`coinmetrics-export` doesn't do that automatically). Run the following command:

```bash
coinmetrics-export print-schema --schema ethereum --storage postgres
```

It outputs necessary SQL statements you need to execute manually on your database.

Then you can start synchronization right away:

```bash
coinmetrics-export export --blockchain ethereum \
  --continue --threads 16 \
  --output-postgres "host=127.0.0.1 user=postgres"
```

This command will not stop unless something happens (like Ethereum daemon or PostgreSQL server goes down), and will continue synchronizing newly arriving blocks indefinitely. It can be safely interrupted, and on subsequent restart it will query the last synchronized block in database and continue sync from there (the `--continue` option).

In case you need to export a manually selected range of blocks you can use `--begin-block` (inclusive) and `--end-block` (exclusive) parameters.

Higher number of threads talking to blockchain daemon (`--threads` parameter) usually increases speed of synchronization, but only up until some limit: test what number on your machine works better. It usually makes sense to make it even higher when connecting to blockchain daemon via network (daemon URL is set with `--api-url` parameter).

## Blockchain export defaults

The utility tries to have sane defaults for most parameters. Note that rewriting history is not supported, and utility is supposed to export main chain only, therefore on most blockchains we have to keep distance from top block (usually called "rewrite limit"), in case the daemon got wrong chain; this distance is specified as negative value of `--end-block` parameter. The exception is Ripple and Stellar, because for them we use history data instead of live node data, and it's never rewritten.

| `--blockchain` | `--api-url` | `--begin-block` | `--end-block` |
|---|---|---|---|
| `cardano` | `http://127.0.0.1:8100/` | `2` | `-1000` |
| `ethereum` | `http://127.0.0.1:8545/` | `0` | `-1000` |
| `nem` | `http://127.0.0.1:7890/` | `1` | `-360` ([rewrite limit](https://nemproject.github.io/#initiating-transactions)) |
| `neo` | `http://127.0.0.1:10332/` | `1` | `-1000` |
| `ripple` | `https://data.ripple.com/` | `32570` ([genesis ledger](https://ripple.com/build/data-api-v2/#genesis-ledger)) | `0` (history data, no rewrites) |
| `stellar` | `http://history.stellar.org/prd/core-live/core_live_001` | `1` | `0` (history data, no rewrites) |

## Note on IOTA

IOTA is harder to synchronize than other blockchains because of its non-linear nature. There's separate command in the works (`export-iota`), but it is not really supported at the moment.
