# Haskell-based CoinMetrics.io tools

These tools are used by CoinMetrics.io team for exporting data from blockchains into analytical databases and monitoring full nodes synchronization state.

## Utilities

* [coinmetrics-export](coinmetrics-export/README.md) - utility for exporting data from blockchains in formats suitable for inserting into analytics databases (SQL, Avro).
* [coinmetrics-monitor](coinmetrics-monitor/README.md) - utility for monitoring blockchain nodes and providing Prometheus-compatible metrics.

## Status

The project is being used in production at Coin Metrics, but many things are in flux or fragile, and docs may be outdated. Command line interface is more or less stable but may change. Please use with caution.

Supported blockchains:

* Binance Chain
* [Bitcoin](https://bitcoin.org/)
* [Cardano](https://www.cardanohub.org/)
* [Cosmos](https://cosmos.network/)
* [EOS](https://eos.io/)
* [Ethereum](https://www.ethereum.org/)
* [Grin](https://grin-tech.org/)
* [IOTA](https://iota.org/)
* [Monero](https://getmonero.org/)
* [NEM](https://nem.io/)
* [NEO](https://neo.org/)
* [Ripple](https://ripple.com/)
* [Stellar](https://www.stellar.org/)
* Generic [Tendermint](https://tendermint.com/)
* [Tezos](https://tezos.com/)
* [Tron](https://tron.network/) (WIP)
* [Waves](https://wavesplatform.com/)

## Binaries

There're no stable releases yet. All binaries are "bleeding edge" ones built on Travis CI.

One easy way to run the tools is to use docker.

Pull the latest version:
```bash
docker pull registry.gitlab.com/coinmetrics/haskell-tools
```

Run e.g. `coinmetrics-export` tool:
```bash
docker run -it --rm --net host registry.gitlab.com/coinmetrics/haskell-tools coinmetrics-export <arguments>
```

## Building from source

### With Stack

Get [stack](https://docs.haskellstack.org/en/stable/install_and_upgrade/).

Run `stack build --install-ghc --copy-bins --local-bin-path <path for executables>`. Executables will be built and placed by specified path.

The code is only tested on Linux (but maybe works on other OSes too).
