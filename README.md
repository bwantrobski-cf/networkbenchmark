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

The examples below show both supported command forms:

```bash
# Installed console script (after `pip install -e .`, usually with .venv active)
networkbenchmark <command> [options]

# Direct launcher from this project folder (requires dependencies in `python`)
python NetBench.py <command> [options]

# A third equivalent form is also available after installation:
python -m networkbenchmark <command> [options]
```

Use the same subcommand and options with either `networkbenchmark` or
`python NetBench.py`. The direct launcher does not require package activation,
but its Python interpreter must have the dependencies installed.

> Note: do not run package internals directly (for example,
> `python networkbenchmark/cli.py`). They use package-relative imports and will
> raise `attempted relative import with no known parent package`.

**Install dependencies into the interpreter you launch with.** The
dependencies (`streamlit`, `pandas`, `ipwhois`) must be installed for the same
Python. With the virtualenv, activate it or use `.venv/bin/python NetBench.py`
explicitly. The `webui` command checks dependencies and reports the exact
install command if they are missing.

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

Start the server on the host that will receive throughput connections. The
installed console script and the direct launcher accept identical options. The
server attempts IPv4 and IPv6 listeners independently; IPv4 remains the
client default, and an unavailable IPv6 address or socket capability is logged
without preventing IPv4 startup.


```bash
# Installed command
networkbenchmark server --bind 0.0.0.0 --udp-port 10000 --tcp-port 11000 --control-port 12000

# Direct launcher
python NetBench.py server --bind 0.0.0.0 --udp-port 10000 --tcp-port 11000 --control-port 12000

# Custom ports and a dedicated log directory
networkbenchmark server --bind 10.0.0.5 --udp-port 20000 --tcp-port 21000 \
    --control-port 22000 --log-dir ./server-logs
python NetBench.py server --bind 10.0.0.5 --udp-port 20000 --tcp-port 21000 \
    --control-port 22000 --log-dir ./server-logs
```

### Client: throughput test

```bash
# TCP upload, one stream, 10 seconds
networkbenchmark throughput --server-ip 10.0.0.5 --tcp --time 10
python NetBench.py throughput --server-ip 10.0.0.5 --tcp --time 10

# IPv6 TCP upload (IPv4 remains the default when -v6 is omitted)
networkbenchmark throughput --server-ip 2001:db8::5 -v6 --tcp --time 10
python NetBench.py throughput --server-ip 2001:db8::5 -v6 --tcp --time 10

# UDP download, four parallel streams, 30 seconds
networkbenchmark throughput --server-ip 10.0.0.5 --udp --reverse -P 4 --time 30
python NetBench.py throughput --server-ip 10.0.0.5 --udp --reverse -P 4 --time 30

# High-rate UDP upload using eight streams across eight worker processes
networkbenchmark throughput --server-ip 10.0.0.5 --udp -P 8 --procs 8 --time 30
python NetBench.py throughput --server-ip 10.0.0.5 --udp -P 8 --procs 8 --time 30

# UDP loss-target tuning: find the highest rate at or below 1% loss
networkbenchmark throughput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
    --acceptable-loss 1.0 --max-duration 30
python NetBench.py throughput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
    --acceptable-loss 1.0 --max-duration 30

# Use non-default server ports and save local logs elsewhere
networkbenchmark throughput --server-ip 10.0.0.5 --udp --start-port 20000 \
    --control-port 22000 --log-dir ./client-logs --time 20
python NetBench.py throughput --server-ip 10.0.0.5 --udp --start-port 20000 \
    --control-port 22000 --log-dir ./client-logs --time 20
```

Every run reports both bitrate (Mbps) and **packets/sec** (send and recv),
plus UDP loss / TCP retransmits and latency-under-load. UDP packets/sec is the
real datagram count; TCP has no application-visible packet boundaries, so its
packets/sec is estimated from throughput assuming a standard 1448-byte MSS.
Live rates are averaged over a short trailing window so the reading stays
steady rather than flickering as stats arrive.

### PMTUD sizing for TCP and UDP

Throughput and `fullbench` run Path MTU Discovery before the throughput tests.
The resulting `path_mtu` is one shared network-path value used for both TCP and
UDP; the tool does not use separate PMTUD values for the two transports.

The packet sizing is then derived differently because TCP and UDP have different
headers:

- **TCP:** the path MTU is reduced by 40 bytes for the IPv4 and TCP headers to
  calculate the TCP MSS. A 1500-byte path MTU therefore produces a 1460-byte
  MSS, and the TCP socket is configured with that MSS when supported.
- **UDP:** the path MTU is reduced by 28 bytes for the IPv4 and UDP headers to
  calculate the maximum benchmark datagram size. A 1500-byte path MTU therefore
  produces a 1472-byte UDP datagram target, including the benchmark's internal
  framing within that datagram.
- **IPv6:** the IPv6 base header is 40 bytes, so IPv6 UDP sizing subtracts 48
  bytes for IPv6 plus UDP, and IPv6 TCP MSS subtracts 60 bytes for IPv6 plus TCP.
  A 1500-byte IPv6 path MTU therefore produces a 1452-byte UDP datagram target
  and a 1440-byte TCP MSS.

The selected address family is recorded as `IPv4` or `IPv6` in local results,
fullbench manifests, PMTUD metadata, and exported NetworkBenchmark payloads.

For example, with `path_mtu=1500`, both transports use the same 1500-byte path
limit, but TCP uses a 1460-byte MSS while UDP targets 1472-byte datagrams. The
selected `path_mtu`, derived TCP MSS, and derived UDP datagram size are written
to the throughput log for verification. If PMTUD is unavailable but ICMP
reachability succeeds, the conservative fallback MTU is used for both.

### Scaling with multiple CPU cores (`--procs`)

At very high packet rates a single Python process becomes CPU-bound (the GIL
serialises per-packet work), capping throughput well below the link. Use
`--procs N` to spread the parallel streams (`-P`) across `N` worker processes
so send/receive work runs on multiple cores:

- Requires more than one stream (`-P > 1`); with a single stream it is a no-op.
- The **server mirrors the client's `--procs`** automatically over the control
  channel — no server-side flag is needed.
- Per-stream UDP loss accounting is preserved (each stream is owned end-to-end
  by one process, backed by shared-memory counters).
- Falls back to the single-process/threaded path on platforms without `fork`
  (e.g. Windows).

A good starting point is `--procs` equal to `-P`, up to the number of physical
cores on the slower of the two hosts.

### UDP loss-target tuning (`--acceptable-loss`)

Unpaced UDP simply blasts as fast as the sender can, which overruns the
receiver's socket buffers and the kernel silently drops the excess — that is
why an unpaced UDP test shows high loss (e.g. 20%+) at a rate far below the
link. (TCP can't do this: flow control + retransmit trade rate for zero loss,
which is also why raw TCP throughput looks higher.)

Instead of a fixed rate, give a **target loss** and let the tool find the
highest UDP rate that stays under it:

```bash
# Find the max UDP upload rate with <= 1% loss, searching for up to 30s
networkbenchmark throughput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
    --acceptable-loss 1.0 --max-duration 30
python NetBench.py throughput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
    --acceptable-loss 1.0 --max-duration 30
```

- `--acceptable-loss PCT` turns on the search (UDP only). `--time` / `--max-duration`
  becomes the **max time budget** for the ramp rather than a fixed run length.
- The search is an **AIMD ramp** (iperf-style): it first probes unpaced to find
  the ceiling, then additively increases the paced send rate while loss stays
  under target and multiplicatively backs off (refining the step) when a trial
  breaches it, converging on the best sustainable rate.
- Works in both directions: for upload the client paces its senders; for
  download (`-R`) the server mirrors the target rate over the control channel.
- The result reports the best sustainable **goodput** and packets/sec at or
  below your loss target, plus the full per-trial rate-vs-loss series.

To raise the ceiling itself on Ubuntu, enlarge the socket buffers
(`net.core.rmem_max` / `wmem_max`), `net.core.netdev_max_backlog`, use more
streams across cores (`-P` / `--procs`), and enable NIC RSS/RPS — unpaced UDP
is packet-rate (CPU) bound, not bandwidth bound.

### Full benchmark matrix (`fullbench`)

For scripting/automation, `fullbench` runs a whole matrix of throughput tests
in one command — TCP and/or UDP, upload and/or download, across a list of
stream counts — stores each test's result locally, and can POST each result as
JSON to an HTTP(S) endpoint (e.g. a Cloudflare Worker that writes to D1).

```bash
# TCP + UDP, upload + download, at 1/8/16/32 streams. UDP uses loss-target
# tuning (<=1% loss, up to 30s each); results POST to an ingestion endpoint.
networkbenchmark fullbench --server-ip 10.0.0.5 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim

# The same fullbench matrix over IPv6
networkbenchmark fullbench --server-ip 2001:db8::5 -v6 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim
python NetBench.py fullbench --server-ip 10.0.0.5 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim

# The same fullbench matrix over IPv6
python NetBench.py fullbench --server-ip 2001:db8::5 -v6 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim

# Preview a small matrix without connecting to the server.
networkbenchmark fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 1,4 --procs 1,2 --time 15 --dry-run
python NetBench.py fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 1,4 --procs 1,2 --time 15 --dry-run

# Run only UDP downloads and write the combined manifest to a custom log folder.
networkbenchmark fullbench --server-ip 10.0.0.5 --transports udp --directions down \
    --streams 4,8 --procs 4,8 --acceptable-loss 0.5 --udp-max-duration 45 \
    --log-dir ./fullbench-logs
python NetBench.py fullbench --server-ip 10.0.0.5 --transports udp --directions down \
    --streams 4,8 --procs 4,8 --acceptable-loss 0.5 --udp-max-duration 45 \
    --log-dir ./fullbench-logs
```

- `--streams` is a comma-separated list of stream counts (e.g. `1,8,16,32`).
- `--procs` is either a single value applied to every run, or a comma-separated
  list **paired** with `--streams` (so `1,8,16,32` + `1,4,8,8` uses 1 proc for
  the 1-stream run, 4 for 8 streams, 8 for 16 and 32).
- `--transports` (default `tcp,udp`) and `--directions` (default `up,down`)
  select which combinations to run.
- `--time` sets the duration for TCP (and non-tuning UDP) tests. Adding
  `--acceptable-loss PCT` switches **UDP** tests to loss-target tuning, with
  `--udp-max-duration` as each search's budget; TCP tests still use `--time`.
- Every test is saved individually to `logs/` (JSON + text), and a combined
  `fullbench` manifest is written summarizing the whole run.
- Throughput and fullbench tests run PMTUD before sending traffic, use the
  discovered path MTU for TCP/UDP sizing, and record `path_mtu`,
  `tcp_mss_bytes`, and `udp_datagram_bytes` in local results and exported
  fullbench payloads. If PMTUD discovery is blocked but ICMP reachability
  succeeds, the test continues with the conservative fallback MTU.
- `--export-url` POSTs each test's JSON result as it completes. Use
  `--export-method` (`POST`/`PUT`), repeatable `--export-header "Key: Value"`
  for auth, and `--export-slim` to drop the bulky `interval_series` /
  `per_stream` arrays for lighter payloads. A failed export is logged but does
  not abort the run.
- `--dry-run` prints the planned matrix without executing.

**Identity / ingestion metadata.** For storing results in a database (e.g. a
Cloudflare D1 table via a results-API Worker), each test can carry identity
fields recorded alongside the metrics:

```bash
networkbenchmark fullbench --server-ip 10.0.0.5 --streams 1,8,16,32 \
    --run-id 137 --vm-name web-01 --cloud aws --environment prod \
    --export-url https://viz.example.com/networkbenchmark-results \
    --export-header "Authorization: Bearer $TOKEN"

python NetBench.py fullbench --server-ip 10.0.0.5 --streams 1,8,16,32 \
    --run-id 137 --vm-name web-01 --cloud aws --environment prod \
    --export-url https://viz.example.com/networkbenchmark-results \
    --export-header "Authorization: Bearer $TOKEN"

# Add a label and use PUT with multiple headers for an idempotent ingestion job.
networkbenchmark fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 8 --label nightly-mk3 --run-id 138 --vm-name web-02 \
    --cloud gcp --environment staging --tool-version 2 --test-type nightly \
    --export-url https://viz.example.com/networkbenchmark-results \
    --export-method PUT --export-header "Authorization: Bearer $TOKEN" \
    --export-header "X-Job: nightly-mk3"
python NetBench.py fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 8 --label nightly-mk3 --run-id 138 --vm-name web-02 \
    --cloud gcp --environment staging --tool-version 2 --test-type nightly \
    --export-url https://viz.example.com/networkbenchmark-results \
    --export-method PUT --export-header "Authorization: Bearer $TOKEN" \
    --export-header "X-Job: nightly-mk3"
```

- `--run-id` is an integer set by your orchestration (e.g. from the results-API
  `/run-id` allocator); it is recorded with every test. NetworkBenchmark does
  not allocate it — scripts own that logic.
- `--vm-name` (default: hostname), `--cloud`, `--environment`, `--tool-version`,
  and `--test-type` (default `networkbenchmark`) are recorded per test too.
- Each individual test gets its own unique `recorded_at` timestamp, plus a
  per-test UUID (`nb_run_id`) and a shared `benchmark_id` for the whole run.

Each exported payload uses the results-API conventions at the top level
(`recorded_at`, `test_type`, `run_id`, `vm_name`, `cloud`, `environment`,
`tool_version`) plus benchmark grouping (`benchmark_id`, `test_index`, `label`,
`nb_run_id`, `transport`, `direction`, `streams`, `procs`, `tuned`) and the full
`result` object (throughput, loss, latency-under-load, bufferbloat, per-stream
counters, per-second `interval_series`, and — for tuned UDP — the per-trial
rate-vs-loss series and best sustainable rate). Ingestion endpoints should read
`result.*` for the metrics and can persist the whole body for full fidelity.

### Diagnostics

Every diagnostic has both an installed-command form and a direct-launcher form.

```bash
# MTR using ICMP, with a 200 ms interval and 20 rounds
networkbenchmark mtr 8.8.8.8 --protocol icmp --interval-ms 200 --count 20
python NetBench.py mtr 8.8.8.8 --protocol icmp --interval-ms 200 --count 20

# MTR using TCP to an HTTPS service
networkbenchmark mtr example.com --protocol tcp --port 443 --max-hops 30 --count 10
python NetBench.py mtr example.com --protocol tcp --port 443 --max-hops 30 --count 10

# IPv6 MTR
networkbenchmark mtr example.com -v6 --protocol tcp --port 443 --max-hops 30 --count 10
python NetBench.py mtr example.com -v6 --protocol tcp --port 443 --max-hops 30 --count 10

# Check ICMP, UDP, and TCP reachability against the throughput server
networkbenchmark connectivity --server-ip 10.0.0.5
python NetBench.py connectivity --server-ip 10.0.0.5

# IPv6 connectivity
networkbenchmark connectivity --server-ip 2001:db8::5 -v6
python NetBench.py connectivity --server-ip 2001:db8::5 -v6

# Use custom diagnostic ports and save the result logs
networkbenchmark connectivity --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./diagnostic-logs
python NetBench.py connectivity --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./diagnostic-logs

# Discover the largest path MTU using DF-marked probes
networkbenchmark pmtud --server-ip 10.0.0.5
python NetBench.py pmtud --server-ip 10.0.0.5

# IPv6 PMTUD
networkbenchmark pmtud --server-ip 2001:db8::5 -v6
python NetBench.py pmtud --server-ip 2001:db8::5 -v6

# Narrow the PMTUD search and use the server's configured UDP port
networkbenchmark pmtud --server-ip 10.0.0.5 --udp-port 20000 \
    --min-size 576 --max-size 2000 --timeout 2 --log-dir ./pmtud-logs
python NetBench.py pmtud --server-ip 10.0.0.5 --udp-port 20000 \
    --min-size 576 --max-size 2000 --timeout 2 --log-dir ./pmtud-logs

# Run the UDP/TCP/ICMP/ESP/NAT-T/GRE reachability matrix
networkbenchmark protocols --server-ip 10.0.0.5
python NetBench.py protocols --server-ip 10.0.0.5

# IPv6 protocol reachability
networkbenchmark protocols --server-ip 2001:db8::5 -v6
python NetBench.py protocols --server-ip 2001:db8::5 -v6

# Use custom ports and a longer timeout for protocol checks
networkbenchmark protocols --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./protocol-logs
python NetBench.py protocols --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./protocol-logs
```

### Web UI

```bash
# Launch on Streamlit's default port
networkbenchmark webui
python NetBench.py webui

# Launch on a custom port
networkbenchmark webui --port 8502
python NetBench.py webui --port 8502
```

The web UI always acts as the **client**, testing against a server started
separately via `networkbenchmark server` or `python NetBench.py server`. It has a persistent Server IP
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

The Web UI's **Use IPv6** selector applies to throughput and all diagnostics;
IPv4 is selected by default. The selected `IPv4`/`IPv6` value is shown in the
connectivity, throughput, and MTR results and is included in saved logs.

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
- The GRE reachability probe emits a standard **GRE version-0** header
  (RFC 2784: zero flags/version, protocol type `0x0800`/IPv4) -- the same
  variant used by default on Cisco IOS, the Linux `ip_gre` module, and Palo
  Alto GRE tunnels -- so it traverses GRE-aware middleboxes like real tunnel
  traffic. (GRE version 1 is PPTP-only and is not used.)
