# coinmetrics-monitor

## Description

`coinmetrics-monitor` polls specified blockchain nodes and provides a HTTP endpoint with Prometheus-compatible metrics.

## Global options

* `--host HOST`, `--port PORT` - endpoint to listen at, `127.0.0.1` and `8080` by default.
* `--global-label NAME=value` - label to be added to all time series. Can be repeated.

## Specifying fullnodes to monitor

See [Blockchains](blockchains.md) page for full nodes specification.

The tool can monitor multiple fullnodes of different types. It is recommended to use specific order of options. For every node the `--blockchain` option must be specified first, then options related to this node such as `--api-url`, and then `--blockchain` again for the next node (even if it is of the same blockchain type as previous node).

In addition to generic blockchain options, the tool allows to set per-node Prometheus labels with `--label NAME=VALUE` option (can be repeated).

## Metrics & labels

The tool reports the following metrics:

* `blockchain_node_sync_height` (override name with `--height-metric`) - height of the latest block
* `blockchain_node_sync_time` (override name with `--time-metric`) - timestamp of the latest block
* `blockchain_node_up` (override name with `--up-metric`) - 1 if node is up, 0 otherwise

Every metric has the following labels attached:

* `blockchain` - type of blockchain (equal to `--blockchain` argument)
* `url` - URL of RPC API (equal to `--api-url` argument)
* `version` - version of fullnode, retrieved via RPC API (not all nodes supported)

## Example

Example command line:

```bash
coinmetrics-monitor \
  --host=0.0.0.0 \
  --port=8000 \
  --blockchain=ethereum \
  --label=name=ethereum \
  --api-url=http://ethereum:8545/ \
  --blockchain=ethereum \
  --label=name=ethereum_classic \
  --api-url=http://ethereum-classic:8545/ \
  --blockchain=monero
```

This example shows setting host and port for HTTP endpoint and three blockchains to monitor: Ethereum and Ethereum Classic with custom URLs and labels, and Monero blockchain with default settings and no additional labels.
