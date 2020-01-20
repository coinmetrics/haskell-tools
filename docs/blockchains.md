# Blockchains

This page documents setting blockchain types and endpoints for the tools.

## General parameters

The tools support the following arguments.

* `--blockchain BLOCKCHAIN` - the type of blockchain to connect to. See below for supported blockchains.
* `--api-url API_URL` - URL of RPC API of the corresponding fullnode, for example: `--api-url http://127.0.0.1:8332/`. Note that every blockchain type sets sensible default for API_URL, using `127.0.0.1` and appropriate port, so specifying `--api-url` manually may be unnecessary.
* `--api-url-username API_URL_USERNAME`, `--api-url-password API_URL_PASSWORD` - allows to set username and password for HTTP authentication for the endpoint.
* `--api-url-insecure` - disable validation of HTTPS certificates (if URL is HTTPS).

## Supported blockchains

|                   Blockchain                  | `--blockchain` |
|-----------------------------------------------|----------------|
| Binance Chain                                 | `binance`      |
| [Bitcoin](https://bitcoin.org/)               | `bitcoin`      |
| [Cardano](https://www.cardanohub.org/)        | `cardano`      |
| [Cosmos](https://cosmos.network/)             | `cosmos`       |
| [EOS](https://eos.io/)                        | `eos`          |
| [EOS](https://eos.io/) with history plugin    | `eos_archive`  |
| [Ethereum](https://www.ethereum.org/)         | `ethereum`     |
| [Grin](https://grin-tech.org/)                | `grin`         |
| [Monero](https://getmonero.org/)              | `monero`       |
| [NEM](https://nem.io/)                        | `nem`          |
| [NEO](https://neo.org/)                       | `neo`          |
| [Ripple](https://ripple.com/)                 | `ripple`       |
| [Stellar](https://www.stellar.org/)           | `stellar`      |
| Generic [Tendermint](https://tendermint.com/) | `tendermint`   |
| [Tezos](https://tezos.com/)                   | `tezos`        |
| [Tron](https://tron.network/) (WIP)           | `tron`         |
| [Waves](https://wavesplatform.com/)           | `waves`        |

## Notes for specific blockchains

### Bitcoin and forks

Most forks of Bitcoin (using the same RPC API) can be exported using `bitcoin` blockchain type. You only need to specify correct port and host with `--api-url`, because default is set for Bitcoin Core (`http://127.0.0.1:8332/`).

Username and password for RPC API can be set with `--api-url-username` and `--api-url-password` options.

### Ethereum

Tested only with [Ethereum Parity](https://github.com/paritytech/parity-ethereum). Works with both Ethereum and Ethereum Classic.

If your Parity node [records tracing information](https://wiki.parity.io/JSONRPC-trace-module), you can specify `--trace` to include it in exported output.

### EOS

`eos` is for normal EOS RPC API. `eos_archive` is for exporting data from [State History Plugin](https://developers.eos.io/eosio-nodeos/docs/state_history_plugin). Output data format of `eos` and `eos_archive` is very different.

### Ripple

Ripple mode requires history server, not fullnode. By default it uses official [history server](https://data.ripple.com/).

### Stellar

Stellar mode requires [history server](https://github.com/stellar/stellar-core/tree/master/src/history), not fullnode. By default it uses official [history servers](https://history.stellar.org/).
