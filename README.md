# NetworkBenchmark

A cross-platform (macOS / Linux / Windows) network throughput and
diagnostics tool. Single executable, runs as **client or server**, driven
by either a **Streamlit web UI** or a **headless CLI**.

Combines iperf3-style parallel-stream throughput testing (upload/download,
TCP/UDP) with bbperf-style calibration and latency-under-load /
bufferbloat measurement, plus a diagnostics suite: MTR (ICMP/UDP/TCP),
connectivity checks, PMTUD, and protocol reachability (UDP/TCP/ICMP,
IPSEC native ESP, IPSEC over UDP/NAT-T, GRE).

This `networkbenchmark/` folder is fully self-contained: it holds all the
code, dependencies, and metadata needed to run and redistribute the tool
(e.g. commit this folder directly to GitHub).

## Project layout

```
networkbenchmark/            <- self-contained project root (commit this)
├── NetBench.py             <- directly-runnable launcher (python NetBench.py ...)
├── pyproject.toml
├── requirements.txt
├── README.md
├── .gitignore
├── logs/                    <- timestamped test result logs (JSON + text)
└── networkbenchmark/        <- the importable Python package
    ├── __init__.py
    ├── __main__.py
    ├── cli.py
    ├── common/  transport/  server/  client/  diagnostics/  webui/
```

## Install

Run these from inside this `networkbenchmark/` project folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Running

There are three equivalent ways to launch the tool:

```bash
# 1. Directly-runnable launcher script (no install / activation required,
#    just the dependencies) -- takes the same arguments as everything below:
python NetBench.py webui

# 2. As a module (requires the package to be importable, e.g. after install):
python -m networkbenchmark webui

# 3. The installed console script (created by `pip install -e .`):
networkbenchmark webui
```

> Note: you cannot run the package's internal files directly
> (e.g. `python networkbenchmark/cli.py`) -- they use package-relative
> imports and will raise `attempted relative import with no known parent
> package`. Use `NetBench.py`, `-m networkbenchmark`, or the console script.

All usage examples below use the `networkbenchmark` console script, but you
can substitute `python NetBench.py` for any of them.

## Raw sockets / privileges

MTR, PMTUD, and native ESP/GRE protocol checks use raw sockets and require
root (macOS/Linux) or Administrator (Windows). The tool probes capabilities
at startup and **gracefully degrades**: unsupported tests are disabled
(greyed out in the UI / rejected with a clear error in the CLI) rather than
crashing. Basic ICMP (via the system `ping`), UDP, TCP, and IPSEC-over-UDP
(NAT-T, port 4500) checks work without elevated privileges.

Because the console script and dependencies live inside the virtualenv,
`sudo networkbenchmark ...` will fail with **command not found** (sudo uses
root's `PATH`, not your venv). Invoke the venv's Python explicitly instead:

```bash
sudo .venv/bin/python NetBench.py webui          # web UI, full diagnostics
sudo .venv/bin/python NetBench.py server ...     # server, full diagnostics

# equivalent alternatives:
sudo .venv/bin/python -m networkbenchmark webui
sudo "$(which networkbenchmark)" webui           # with the venv activated
```

## Usage

### Server

```bash
networkbenchmark server --bind 0.0.0.0 --udp-port 10000 --tcp-port 11000 --control-port 12000
```

### Client: throughput test

```bash
# Upload, TCP, single stream, 10s
networkbenchmark throughput --server-ip 10.0.0.5 --time 10

# Download, UDP, 4 parallel streams, 30s
networkbenchmark throughput --server-ip 10.0.0.5 --udp -R -P 4 --time 30
```

### Diagnostics

```bash
networkbenchmark mtr 8.8.8.8 --protocol icmp --interval-ms 200
networkbenchmark mtr example.com --protocol tcp --port 443
networkbenchmark connectivity --server-ip 10.0.0.5
networkbenchmark pmtud --server-ip 10.0.0.5
networkbenchmark protocols --server-ip 10.0.0.5
```

### Web UI

```bash
networkbenchmark webui
```

The web UI always acts as the **client**, testing against a server started
separately via `networkbenchmark server`. It has a persistent Server IP
field (top of page) plus a "Check Connectivity" button, a default
**NetworkBenchmark** tab for throughput tests, a **Diagnostics** tab
(MTR / PMTUD / Protocol Checks), and a **Settings** tab for the UDP/TCP/
control port configuration.

## Ports

| Purpose                                   | Default |
|--------------------------------------------|---------|
| Control channel (throughput orchestration) | 12000   |
| UDP base port (connectivity + streams)     | 10000   |
| TCP base port (connectivity + streams)     | 11000   |
| IPSEC UDP-over-ESP (NAT-T, fixed)           | 4500    |

Parallel streams (`-P N`) use sequential ports starting at the configured
UDP/TCP base port (`base_port + stream_index`). All ports are configurable
on the server via CLI flags / the Settings tab, and the client must be told
the server's base port via `--start-port` (CLI) or the Settings tab (UI).

## Logs

Every completed test writes a timestamped JSON + text log pair into this
project's `logs/` folder (alongside the package), regardless of the current
working directory:

```
logs/20260706-114500_throughput.json
logs/20260706-114500_throughput.log
```

Override the location per-run with `--log-dir <path>`.

## Notes on accuracy

- TCP retransmit counts are read via `TCP_INFO` and are precise on Linux;
  macOS/Windows report `n/a` (no stable public API from Python).
- MTR/PMTUD/ESP/GRE are implemented with raw sockets using the Python
  standard library only (no scapy dependency).
