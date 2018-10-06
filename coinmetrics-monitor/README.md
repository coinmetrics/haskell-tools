# coinmetrics-monitor

## Description

`coinmetrics-monitor` polls specified blockchain nodes and provides a HTTP endpoint with Prometheus-compatible metrics.

## Example

Example command line:

```bash
coinmetrics-monitor \
  --host=0.0.0.0 \
  --port=80 \
  --blockchain=ethereum \
  --api-url=http://ethereum:8545/ \
  --blockchain=ethereum \
  --name=ethereum_classic \
  --api-url=http://ethereum-classic:8545/ \
  --blockchain=monero
```

This example shows setting host and port for HTTP endpoint (default is 127.0.0.1:8080) and three blockchains to monitor: Ethereum and Ethereum Classic with custom URLs and metric names (otherwise both blockchains of `ethereum` type would have the default `http://127.0.0.1:8545/` URL and `ethereum` metric name) and Monero blockchain with default settings.
