"""Shared constants / defaults for NetworkBenchmark."""

APP_NAME = "networkbenchmark"

# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------
# Control channel used to negotiate/orchestrate throughput tests and relay
# receiver-side / sender-side stats between client and server.
DEFAULT_CONTROL_PORT = 12000

# Base ports used BOTH for the quick "check connectivity" echo probes AND as
# the base port for throughput-test parallel data streams (stream i uses
# base_port + i, opened on demand for the duration of a throughput test).
DEFAULT_UDP_PORT = 10000
DEFAULT_TCP_PORT = 11000

# Fixed well-known port for UDP-encapsulated ESP (NAT-T), per RFC 3948.
IPSEC_NATT_PORT = 4500

# IP protocol numbers
IPPROTO_ESP = 50
IPPROTO_GRE = 47

# ---------------------------------------------------------------------------
# Throughput test timing
# ---------------------------------------------------------------------------
SAMPLE_INTERVAL_SEC = 0.1
CALIBRATION_TIME_SEC = 2.0
STDOUT_INTERVAL_SEC = 1.0
SOCKET_TIMEOUT_SEC = 30
CONTROL_RECV_POLL_SEC = 0.05
PROBE_INTERVAL_SEC = 0.1
MAX_RUN_TIME_FAILSAFE_SEC = 3600
DEFAULT_TEST_DURATION_SEC = 10
DEFAULT_STREAMS = 1

BUFSZ = 128 * 1024
UDP_PAYLOAD_SIZE = 1200
TCP_CHUNK_SIZE = 128 * 1024

# Magic tokens for demuxing traffic that lands on the shared base ports.
CONNECTIVITY_MAGIC = b"NBCONN01"
DATA_STREAM_MAGIC = b"NBDATA01"

# ---------------------------------------------------------------------------
# MTR
# ---------------------------------------------------------------------------
MTR_MIN_INTERVAL_MS = 100
MTR_MAX_INTERVAL_MS = 1000
MTR_DEFAULT_INTERVAL_MS = 200
MTR_DEFAULT_MAX_HOPS = 30
MTR_DEFAULT_TIMEOUT_SEC = 1.0
MTR_PROTOCOLS = ("icmp", "udp", "tcp")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR_NAME = "logs"

TEST_TYPE_THROUGHPUT = "throughput"
TEST_TYPE_MTR = "mtr"
TEST_TYPE_CONNECTIVITY = "connectivity"
TEST_TYPE_PMTUD = "pmtud"
TEST_TYPE_PROTOCOLS = "protocols"
