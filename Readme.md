# SMA Based Experimentes for OTEL / WASM Experiments

We run two complementary experiment suites. 

## Treatments

### End-to-end suite
#### Runtime variants:
- baseline: all key services in container form (baseline overlay).
- wasmtime-vanilla: shipping moved to Wasm/Wasmtime (wasi-vanilla overlay).
- wasmtime-grpc: shipping + recommendation moved to Wasm (wasi-grpc overlay).
- wasmtime-tcp: shipping + recommendation + cart moved to Wasm (wasi-tcp overlay).

#### Workload generator: 
Locust shopping-flow traffic through frontend (SMA telelocust module).

#### Load/stress conditions: 
workload profiles light/regular/heavy plus system noisy-neighbor stress-ng levels none/low/medium/high.

### Measurements

Collected by SMA (Prometheus-backed) in both suites.
Observation windows: 60s pre and 60s post; max duration 480s.

- External wall power (Shelly), node power (Scaphandre).
- Node CPU, memory, disk IO, network and overlay network.
- Pod/container CPU, memory, power (Kepler + Scaphandre).
- WASI-specific and Kirin/process-group CPU/power/memory.
- CPU throttling and platform overhead (kubelet/containerd/network stack via process-exporter).

Output format: CSV reports in run-specific folders under e2e_reports and isolation_reports.
Environment

### Cluster
- 1 control-plane node (orchestrator/workload gen) + 1+ bare-metal SUT nodes.
-  Debian 13+, Kubernetes 1.32+, containerd 2.2+, Flannel, Prometheus stack + Kepler + Scaphandre + process-exporter.

## Running

Run on the control-plane node, from `orchestrator/`:

```sh
pipenv install
cp enviroment.yaml.example enviroment.yaml   # fill in hosts, paths and image registry
# set the Prometheus address in end2end_sma_config.yaml / isolation_sma_config.yaml
pipenv run python end2end.py    # end-to-end suite
pipenv run python isolated.py   # isolated suite
```
