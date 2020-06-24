# Haskell-based CoinMetrics.io tools

These tools are used by CoinMetrics.io team for exporting data from blockchains into analytical databases and monitoring full nodes synchronization state.

## Utilities

* [coinmetrics-export](docs/coinmetrics-export.md) - utility for exporting data from blockchains in formats suitable for inserting into analytics databases (SQL, Avro).
* [coinmetrics-monitor](docs/coinmetrics-monitor.md) - utility for monitoring blockchain nodes and providing Prometheus-compatible metrics.

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

There're no stable releases yet. All binaries are "bleeding edge".

One easy way to run the tools is to use docker.

Pull the latest version:
```bash
docker pull coinmetrics/haskell-tools
```

Run e.g. `coinmetrics-export` tool:
```bash
docker run -it --rm --net host coinmetrics/haskell-tools coinmetrics-export <arguments>
```

## Building from source

### With Nix

```bash
nix build -Lf ./release.nix bins
```

### With Stack

Get [stack](https://docs.haskellstack.org/en/stable/install_and_upgrade/).

Run `stack build --install-ghc --copy-bins --local-bin-path <path for executables>`. Executables will be built and placed by specified path.

Note: Stellar support requires additional dependencies ([xdrpp](https://github.com/xdrpp/xdrpp) and Stellar XDR headers built with XDR compiler). Without them the build will fail by default. You can either disable Stellar support with Stack command-line option `--flag coinmetrics-all-blockchains:-stellar`, or build with Nix which fetches and builds all dependencies automatically.

The code is only tested on Linux (but maybe works on other OSes too).
