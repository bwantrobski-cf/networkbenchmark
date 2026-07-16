"""NPERF: a cross-platform network throughput and diagnostics tool.

Provides a single executable that can run as a client or server, driven
either by a Streamlit web UI or a headless CLI. Combines iperf3-style
parallel-stream throughput testing (upload/download, TCP/UDP) with
bbperf-style bufferbloat / latency-under-load measurement, plus a
diagnostics suite (MTR, connectivity checks, PMTUD, protocol reachability).
"""

__version__ = "0.1.0"
