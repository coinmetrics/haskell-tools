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
* Generic [Rosetta API](https://www.rosetta-api.org/) (WIP)
* [Stellar](https://www.stellar.org/)
* Generic [Tendermint](https://tendermint.com/)
* [Tezos](https://tezos.com/)
* [Tron](https://tron.network/)
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

Building with Nix is recommended, because dependencies are fetched and built automatically.

### With Nix

```bash
nix build -Lf ./release.nix bins
```

### With Stack + Nix

```bash
stack build --nix
```

### With Stack

```bash
stack build
```

Required dependencies: `zlib`, `libpq`.

Stellar export also requires [xdrpp](https://github.com/xdrpp/xdrpp) and Stellar XDR headers built with XDR compiler, which may be hard to build manually. By default Stellar support is enabled, so build will fail without them. You can disable Stellar support if you don't need it: `stack build --flag coinmetrics-all-blockchains:-stellar`. If you do need it, please use one of the Nix build methods above (with Stack or without).

The code is only tested on Linux (but maybe works on other OSes too).
