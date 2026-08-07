# NPERF

A cross-platform (macOS / Linux / Windows) network throughput and
diagnostics tool. Single executable, runs as **client or server**, driven
by either a **Streamlit web UI** or a **headless CLI**.

Combines iperf3-style parallel-stream throughput testing (upload/download,
TCP/UDP) with bbperf-style calibration and latency-under-load /
bufferbloat measurement, plus a diagnostics suite: MTR (ICMP/UDP/TCP),
connectivity checks, PMTUD, and protocol reachability (UDP/TCP/ICMP,
IPSEC native ESP, IPSEC over UDP/NAT-T, GRE).

This project folder is fully self-contained: it holds all the
code, dependencies, and metadata needed to run and redistribute NPERF
(e.g. commit this folder directly to GitHub).

## Project layout

```
<project-root>/             <- self-contained project root (commit this)
├── nperf.py             <- directly-runnable launcher (python nperf.py ...)
├── pyproject.toml
├── requirements.txt
├── README.md
├── .gitignore
├── logs/                    <- timestamped test result logs (JSON + text)
└── nperf/                   <- the importable Python package
    ├── __init__.py
    ├── __main__.py
    ├── cli.py
    ├── common/  transport/  server/  client/  diagnostics/  webui/
```

## Install

Run these from inside this project folder:

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
nperf <command> [options]

# Direct launcher from this project folder (requires dependencies in `python`)
python nperf.py <command> [options]

# A third equivalent form is also available after installation:
python -m nperf <command> [options]
```

Use the same subcommand and options with either `nperf` or
`python nperf.py`. The direct launcher does not require package activation,
but its Python interpreter must have the dependencies installed.

> Note: do not run package internals directly (for example,
> `python nperf/cli.py`). They use package-relative imports and will
> raise `attempted relative import with no known parent package`.

**Install dependencies into the interpreter you launch with.** The
dependencies (`streamlit`, `pandas`, `ipwhois`) must be installed for the same
Python. With the virtualenv, activate it or use `.venv/bin/python nperf.py`
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
`sudo nperf ...` will fail with **command not found** (sudo uses
root's `PATH`, not your venv). Invoke the venv's Python explicitly instead:

```bash
sudo .venv/bin/python nperf.py webui          # web UI, full diagnostics
sudo .venv/bin/python nperf.py server ...     # server, full diagnostics

# equivalent alternatives:
sudo .venv/bin/python -m nperf webui
sudo "$(which nperf)" webui           # with the venv activated
```

## Usage

### Server

Start the server on the host that will receive throughput connections. The
installed console script and the direct launcher accept identical options. The
server attempts IPv4 and IPv6 listeners independently; IPv4 remains the
client default, and an unavailable IPv6 address or socket capability is logged
without preventing IPv4 startup.


```bash
# Start the server with its default bind address and ports
python nperf.py server

# Equivalent installed command
nperf server

# Explicitly specifying the defaults
nperf server --bind 0.0.0.0 --udp-port 10000 --tcp-port 11000 --control-port 12000

# Direct launcher with explicit defaults
python nperf.py server --bind 0.0.0.0 --udp-port 10000 --tcp-port 11000 --control-port 12000

# Custom ports and a dedicated log directory
nperf server --bind 10.0.0.5 --udp-port 20000 --tcp-port 21000 \
    --control-port 22000 --log-dir ./server-logs
python nperf.py server --bind 10.0.0.5 --udp-port 20000 --tcp-port 21000 \
    --control-port 22000 --log-dir ./server-logs
```

### Client: throughput test

```bash
# On the server host, run first:
python nperf.py server

# On the client host: TCP upload with 16 streams across 4 worker processes/cores
python nperf.py tput --server-ip 10.0.0.5 --tcp -P 16 --procs 4 --time 30

# TCP upload, one stream, 10 seconds
nperf tput --server-ip 10.0.0.5 --tcp --time 10
python nperf.py tput --server-ip 10.0.0.5 --tcp --time 10

# IPv6 TCP upload (IPv4 remains the default when -v6 is omitted)
nperf tput --server-ip 2001:db8::5 -v6 --tcp --time 10
python nperf.py tput --server-ip 2001:db8::5 -v6 --tcp --time 10

# UDP download, four parallel streams, 30 seconds
nperf tput --server-ip 10.0.0.5 --udp --reverse -P 4 --time 30
python nperf.py tput --server-ip 10.0.0.5 --udp --reverse -P 4 --time 30

# High-rate UDP upload using eight streams across eight worker processes
nperf tput --server-ip 10.0.0.5 --udp -P 8 --procs 8 --time 30
python nperf.py tput --server-ip 10.0.0.5 --udp -P 8 --procs 8 --time 30

# UDP loss-target tuning: find the highest rate at or below 1% loss
nperf tput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
    --acceptable-loss 1.0 --max-duration 30
python nperf.py tput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
    --acceptable-loss 1.0 --max-duration 30

# Use non-default server ports and save local logs elsewhere
nperf tput --server-ip 10.0.0.5 --udp --start-port 20000 \
    --control-port 22000 --log-dir ./client-logs --time 20
python nperf.py tput --server-ip 10.0.0.5 --udp --start-port 20000 \
    --control-port 22000 --log-dir ./client-logs --time 20
```

Every run reports both bitrate (Mbps) and **packets/sec** (send and recv),
plus UDP loss / TCP retransmits and latency-under-load. UDP packets/sec is the
real datagram count; TCP has no application-visible packet boundaries, so its
packets/sec is estimated from throughput assuming a standard 1448-byte MSS.
Live rates are averaged over a short trailing window so the reading stays
steady rather than flickering as stats arrive.

UDP setup or data-plane attempts that send packets but receive none are retried
up to three total attempts with 0.5-second and 1.0-second exponential-backoff
delays. The CLI fullbench table displays TCP retransmitted segments for TCP
rows and UDP loss percentage for UDP rows. TCP retransmits are sampled from the
application-data sending endpoint: Linux uses `TCP_INFO.tcpi_total_retrans`
and macOS uses `TCP_CONNECTION_INFO.tcpi_txretransmitpackets`.

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
fullbench manifests, PMTUD metadata, and exported NPERF payloads.

The PMTUD discovery chain first uses DF-marked UDP probes. If UDP PMTUD fails,
NPERF sends DF-marked ICMP echo probes, starting at a 1450-byte packet and
adjusting by 50 bytes until it brackets the limit, then binary-searching for
the largest successful packet. If both discovery methods fail, NPERF continues
with a fixed 1300-byte MTU and records `path_mtu_source=fixed_fallback` in the
result metadata.

For example, with `path_mtu=1500`, both transports use the same 1500-byte path
limit, but TCP uses a 1460-byte MSS while UDP targets 1472-byte datagrams. The
selected `path_mtu`, derived TCP MSS, and derived UDP datagram size are written
to the throughput log for verification.

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
nperf tput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
    --acceptable-loss 1.0 --max-duration 30
python nperf.py tput --server-ip 10.0.0.5 --udp -P 4 --procs 4 \
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
nperf fullbench --server-ip 10.0.0.5 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim

# The same fullbench matrix over IPv6
nperf fullbench --server-ip 2001:db8::5 -v6 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim
python nperf.py fullbench --server-ip 10.0.0.5 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim

# The same fullbench matrix over IPv6
python nperf.py fullbench --server-ip 2001:db8::5 -v6 \
    --streams 1,8,16,32 --procs 1,4,8,8 \
    --acceptable-loss 1.0 --udp-max-duration 30 \
    --export-url https://viz.example.com/ingest \
    --export-header "Authorization: Bearer $TOKEN" --export-slim

# Preview a small matrix without connecting to the server.
nperf fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 1,4 --procs 1,2 --time 15 --dry-run
python nperf.py fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 1,4 --procs 1,2 --time 15 --dry-run

# Run only UDP downloads and write the combined manifest to a custom log folder.
nperf fullbench --server-ip 10.0.0.5 --transports udp --directions down \
    --streams 4,8 --procs 4,8 --acceptable-loss 0.5 --udp-max-duration 45 \
    --log-dir ./fullbench-logs
python nperf.py fullbench --server-ip 10.0.0.5 --transports udp --directions down \
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
- Throughput and fullbench tests run the UDP/ICMP/fixed PMTUD discovery chain
  before sending traffic, use the selected path MTU for TCP/UDP sizing, and
  record `path_mtu`, `path_mtu_source`, `tcp_mss_bytes`, and `udp_datagram_bytes`
  in local results and exported fullbench payloads.
- `--export-url` POSTs each test's JSON result as it completes. Use
  `--export-method` (`POST`/`PUT`), repeatable `--export-header "Key: Value"`
  for auth, and `--export-slim` to drop the bulky `interval_series`,
  `per_stream`, and `source_ports` fields for lighter payloads. A failed export
  is logged but does not abort the run.
- `--dry-run` prints the planned matrix without executing.

**Identity / ingestion metadata.** For storing results in a database (e.g. a
Cloudflare D1 table via a results-API Worker), each test can carry identity
fields recorded alongside the metrics:

```bash
nperf fullbench --server-ip 10.0.0.5 --streams 1,8,16,32 \
    --run-id 137 --vm-name web-01 --cloud aws --environment prod \
    --export-url https://viz.example.com/nperf-results \
    --export-header "Authorization: Bearer $TOKEN"

python nperf.py fullbench --server-ip 10.0.0.5 --streams 1,8,16,32 \
    --run-id 137 --vm-name web-01 --cloud aws --environment prod \
    --export-url https://viz.example.com/nperf-results \
    --export-header "Authorization: Bearer $TOKEN"

# Add a label and use PUT with multiple headers for an idempotent ingestion job.
nperf fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 8 --label nightly-mk3 --run-id 138 --vm-name web-02 \
    --cloud gcp --environment staging --tool-version 2 --test-type nightly \
    --export-url https://viz.example.com/nperf-results \
    --export-method PUT --export-header "Authorization: Bearer $TOKEN" \
    --export-header "X-Job: nightly-mk3"
python nperf.py fullbench --server-ip 10.0.0.5 --transports tcp --directions up \
    --streams 8 --label nightly-mk3 --run-id 138 --vm-name web-02 \
    --cloud gcp --environment staging --tool-version 2 --test-type nightly \
    --export-url https://viz.example.com/nperf-results \
    --export-method PUT --export-header "Authorization: Bearer $TOKEN" \
    --export-header "X-Job: nightly-mk3"
```

- `--run-id` is an integer set by your orchestration (e.g. from the results-API
  `/run-id` allocator); it is recorded with every test. NPERF does
  not allocate it — scripts own that logic.
- `--vm-name` (default: hostname), `--cloud`, `--environment`, `--tool-version`,
  and `--test-type` (default `nperf`) are recorded per test too.
- Each individual test gets its own unique `recorded_at` timestamp, plus a
  per-test UUID (`nb_run_id`) and a shared `benchmark_id` for the whole run.

Each configured export sends **one JSON object per completed fullbench test**
to `--export-url`. There is no outer array and no separate fullbench envelope in
the HTTP request. The request uses `Content-Type: application/json`; the method
is `POST` by default or `PUT` when selected with `--export-method`.

The top-level exported object has this structure:

```json
{
  "recorded_at": "2026-07-16T17:02:31.123456+00:00",
  "test_type": "nperf",
  "ip_version": "IPv4",
  "run_id": 137,
  "vm_name": "web-01",
  "cloud": "aws",
  "environment": "prod",
  "tool_version": 1,
  "benchmark_id": "uuid-for-the-fullbench-run",
  "test_index": 1,
  "test_count": 8,
  "label": "nightly-mk3",
  "crypto_setting": "nightly-mk3",
  "transport": "tcp",
  "direction": "up",
  "streams": 8,
  "procs": 2,
  "tuned": false,
  "path_mtu": 1500,
  "path_mtu_source": "udp_pmtud",
  "pmtud_fallback_used": false,
  "pmtud_fallback_reason": null,
  "reported_next_hop_mtu": null,
  "tcp_mss_bytes": 1460,
  "udp_datagram_bytes": 1472,
  "nb_run_id": "uuid-for-this-test",
  "ok": true,
  "error": null,
  "send_mbps": 940.2,
  "recv_mbps": 938.7,
  "send_pps": 80400.0,
  "recv_pps": 80270.0,
  "loss_pct": null,
  "retransmits": 12,
  "log_json": "logs/throughput.json",
  "result": {
    "run_id": "uuid-for-this-test",
    "server": "10.0.0.5",
    "ip_version": "IPv4",
    "transport": "tcp",
    "direction": "up",
    "streams": 8,
    "procs": 2,
    "duration_sec": 10,
    "path_mtu": 1500,
    "tcp_mss_bytes": 1460,
    "udp_datagram_bytes": 1472,
    "totals": {
      "bytes_sent": 1175250000,
      "packets_sent": 900000,
      "bytes_received": 0,
      "packets_received": 0,
      "packets_lost": 0,
      "retransmits": 12
    },
    "send_mbps": 940.2,
    "recv_mbps": 938.7,
    "send_pps": 80400.0,
    "recv_pps": 80270.0,
    "loss_pct": null,
    "retransmits": 12,
    "unloaded_rtt_ms": 12.4,
    "loaded_rtt": {
      "count": 100,
      "avg": 18.1,
      "best": 12.2,
      "worst": 31.7,
      "stddev": 3.4,
      "jitter": 2.1
    },
    "bufferbloat": {
      "bdp_bytes": 1462500,
      "buffered_bytes": 1068750,
      "bloat_factor": 0.73
    },
    "per_stream": [],
    "interval_series": [],
    "source_ports": {},
    "tuning": null,
    "log_json": "logs/throughput.json",
    "log_text": "logs/throughput.txt"
  }
}
```

**Top-level fields.** `recorded_at` is the per-test UTC timestamp. `run_id`,
`vm_name`, `cloud`, `environment`, `tool_version`, and `test_type` are the
orchestration/ingestion fields. `benchmark_id` groups all tests from one
fullbench run; `test_index` and `test_count` identify the test's position in
that matrix. `nb_run_id` is the unique throughput-test UUID. `ok` and `error`
report execution status. The duplicated rate, loss, retransmit, MTU, and
packet-sizing fields are convenient ingestion columns; the complete metrics
remain under `result`.

**`result` fields.** The result contains the test identity (`run_id`, `server`,
`ip_version`, `transport`, `direction`, `streams`, `procs`, `duration_sec`),
packet sizing (`path_mtu`, `tcp_mss_bytes`, `udp_datagram_bytes`), aggregate
counters (`totals.bytes_sent`, `packets_sent`, `bytes_received`,
`packets_received`, `packets_lost`, `retransmits`), rates (`send_mbps`,
`recv_mbps`, `send_pps`, `recv_pps`), `loss_pct`, TCP `retransmits`,
`unloaded_rtt_ms`, the `loaded_rtt` statistics object (`count`, `avg`, `best`,
`worst`, `stddev`, `jitter`), `bufferbloat`, and the local `log_json`/`log_text`
paths. `interval_series` contains per-interval measurements, `per_stream`
contains stream counter snapshots including `index`, byte/packet counters,
loss, retransmits, and source port, and `source_ports` maps stream indexes to
local source ports. These three fields may be omitted by `--export-slim`.

For tuned UDP tests, `tuning` is an object with `acceptable_loss_pct`,
`max_duration_sec`, `probe`, `trials`, `best`, and `best_effort`. Trial objects
contain the target rate, offered/goodput Mbps and packets/sec, loss percentage,
and local/peer interval durations. `best` is the highest-goodput trial meeting
the loss target; `best_effort` is the highest-goodput trial when no trial meets
it. For non-tuned tests, `tuning` is `null`.

**PMTUD fields.** `path_mtu_source` is `udp_pmtud`, `icmp_pmtud`, or
`fixed_fallback`. `pmtud_fallback_used` indicates whether UDP PMTUD was not the
selected method. When the fixed fallback is used, `path_mtu` is 1300 and
`pmtud_fallback_reason` explains why discovery failed. The nested `pmtud`
diagnostic object is used in local results and manifests but is intentionally
removed from the HTTP payload; the promoted PMTUD fields above preserve the
important ingestion metadata.

**Slim exports.** `--export-slim` removes only `result.interval_series`,
`result.per_stream`, and `result.source_ports`. All top-level identity fields,
aggregate metrics, tuning data, PMTUD metadata, and the remaining result fields
are still sent.

The local fullbench manifest is separate from the HTTP payload. It contains the
shared run metadata, fullbench configuration, PMTUD information, and a `tests`
array of all per-test records. The exporter sends each successful test record
individually; it does not send that manifest as one request. Ingestion endpoints
should read `result.*` for metrics and can persist the whole request body for
full fidelity.

### Diagnostics

Every diagnostic has both an installed-command form and a direct-launcher form.

```bash
# MTR using ICMP, with a 200 ms interval and 20 rounds
nperf mtr 8.8.8.8 --protocol icmp --interval-ms 200 --count 20
python nperf.py mtr 8.8.8.8 --protocol icmp --interval-ms 200 --count 20

# MTR using TCP to an HTTPS service
nperf mtr example.com --protocol tcp --port 443 --max-hops 30 --count 10
python nperf.py mtr example.com --protocol tcp --port 443 --max-hops 30 --count 10

# IPv6 MTR
nperf mtr example.com -v6 --protocol tcp --port 443 --max-hops 30 --count 10
python nperf.py mtr example.com -v6 --protocol tcp --port 443 --max-hops 30 --count 10

# Check ICMP, UDP, and TCP reachability against the throughput server
nperf connectivity --server-ip 10.0.0.5
python nperf.py connectivity --server-ip 10.0.0.5

# IPv6 connectivity
nperf connectivity --server-ip 2001:db8::5 -v6
python nperf.py connectivity --server-ip 2001:db8::5 -v6

# Use custom diagnostic ports and save the result logs
nperf connectivity --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./diagnostic-logs
python nperf.py connectivity --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./diagnostic-logs

# Discover the largest path MTU using DF-marked probes
nperf pmtud --server-ip 10.0.0.5
python nperf.py pmtud --server-ip 10.0.0.5

# IPv6 PMTUD
nperf pmtud --server-ip 2001:db8::5 -v6
python nperf.py pmtud --server-ip 2001:db8::5 -v6

# Narrow the PMTUD search and use the server's configured UDP port
nperf pmtud --server-ip 10.0.0.5 --udp-port 20000 \
    --min-size 576 --max-size 2000 --timeout 2 --log-dir ./pmtud-logs
python nperf.py pmtud --server-ip 10.0.0.5 --udp-port 20000 \
    --min-size 576 --max-size 2000 --timeout 2 --log-dir ./pmtud-logs

# Run the UDP/TCP/ICMP/ESP/NAT-T/GRE reachability matrix
nperf protocols --server-ip 10.0.0.5
python nperf.py protocols --server-ip 10.0.0.5

# IPv6 protocol reachability
nperf protocols --server-ip 2001:db8::5 -v6
python nperf.py protocols --server-ip 2001:db8::5 -v6

# Use custom ports and a longer timeout for protocol checks
nperf protocols --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./protocol-logs
python nperf.py protocols --server-ip 10.0.0.5 --udp-port 20000 \
    --tcp-port 21000 --timeout 5 --log-dir ./protocol-logs
```

### Web UI

```bash
# Launch on Streamlit's default port
nperf webui
python nperf.py webui

# Launch on a custom port
nperf webui --port 8502
python nperf.py webui --port 8502
```

The web UI always acts as the **client**, testing against a server started
separately via `nperf server` or `python nperf.py server`. It has a persistent Server IP
field (top of page) plus a "Check Connectivity" button, a default
**NPERF** page for single throughput tests, a **Fullbench** page
for running a TCP/UDP, upload/download, stream-count matrix, a
**Diagnostics** page (MTR / PMTUD / Protocol Checks), and a **Settings** page
for the UDP/TCP/control port configuration.

## Ports

| Purpose                                   | Default |
|--------------------------------------------|---------|
| Control channel (throughput orchestration) | 12000   |
| UDP base port (connectivity + streams)     | 10000   |
| TCP base port (connectivity + streams)     | 11000   |
| IPSEC UDP-over-ESP (NAT-T, fixed)           | 4500    |

Parallel streams (`-P N`) use sequential ports starting at the configured
UDP/TCP base port (`base_port + stream_index`). Each stream is an independent
concurrent data flow; `--procs` only controls how those streams are distributed
across worker processes and does not replace the stream count. All ports are
configurable on the server via CLI flags / the Settings tab, and the client
must be told the server's base port via `--start-port` (CLI) or the Settings
tab (UI).

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
