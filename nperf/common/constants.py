"""Shared constants / defaults for NPERF."""

APP_NAME = "nperf"

# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------
# Control channel used to negotiate/orchestrate throughput tests and relay
# receiver-side / sender-side stats between client and server.
DEFAULT_CONTROL_PORT = 12000

# Base ports used BOTH for the quick "check connectivity" echo probes AND as
# the base port for throughput-test parallel data streams (opened on demand,
# skipping ports reserved by the server for the same transport).
DEFAULT_UDP_PORT = 10000
DEFAULT_TCP_PORT = 11000

# Fixed well-known port for UDP-encapsulated ESP (NAT-T), per RFC 3948.
IPSEC_NATT_PORT = 4500

# IP protocol numbers
IPPROTO_ESP = 50
IPPROTO_GRE = 47

# Standard GRE version-0 header (RFC 2784), as used by default on Cisco IOS,
# the Linux `ip_gre` module, and Palo Alto GRE tunnels. First 2 bytes are the
# flags + version field (all zero: no checksum/key/sequence, version 0); next
# 2 bytes are the Protocol Type, 0x0800 = IPv4. This 4-byte header precedes our
# reachability payload so the probe looks like real GRE v0 on the wire.
GRE_V0_PROTOCOL_TYPE_IPV4 = 0x0800
GRE_V0_PROTOCOL_TYPE_IPV6 = 0x86DD
GRE_V0_HEADER = b"\x00\x00" + GRE_V0_PROTOCOL_TYPE_IPV4.to_bytes(2, "big")
GRE_V0_HEADER_LEN = len(GRE_V0_HEADER)

# ---------------------------------------------------------------------------
# Throughput test timing
# ---------------------------------------------------------------------------
SAMPLE_INTERVAL_SEC = 0.1
PEER_STATS_INTERVAL_SEC = 0.5
UDP_RETRY_ATTEMPTS = 3
UDP_RETRY_BACKOFF_SEC = 0.5
# Reported send/recv rates are averaged over a trailing window rather than a
# single SAMPLE_INTERVAL_SEC delta. This smooths out the burstiness of the
# peer-relayed direction (peer_stats share the control channel with the
# saturating data flow and arrive in bursts) so the live rate no longer
# oscillates between 0 and a catch-up spike.
RATE_WINDOW_SEC = 1.0
CALIBRATION_TIME_SEC = 2.0
STDOUT_INTERVAL_SEC = 1.0
SOCKET_TIMEOUT_SEC = 30
FINAL_STATS_TIMEOUT_SEC = 60
WORKER_DRAIN_TIMEOUT_SEC = 10
# TCP receivers must consume queued data and retransmissions before seeing EOF.
# Leave room for the final control exchange within FINAL_STATS_TIMEOUT_SEC.
TCP_RECEIVER_DRAIN_TIMEOUT_SEC = 45
CONTROL_RECV_POLL_SEC = 0.05
PROBE_INTERVAL_SEC = 0.1
MAX_RUN_TIME_FAILSAFE_SEC = 3600
DEFAULT_TEST_DURATION_SEC = 10
DEFAULT_STREAMS = 1
MAX_PARALLEL_STREAMS = 1024

BUFSZ = 128 * 1024
UDP_PAYLOAD_SIZE = 1200
TCP_CHUNK_SIZE = 128 * 1024
FALLBACK_PATH_MTU = 1300

# PMTUD bounds are total IP packet sizes. Standard probing stops at Ethernet
# MTU unless a caller explicitly requests a larger value, up to the jumbo cap.
PMTUD_MIN_MTU = 68
PMTUD_DEFAULT_MAX_MTU = 1500
PMTUD_JUMBO_MAX_MTU = 9000

# ---------------------------------------------------------------------------
# UDP loss-target tuning (multiplicative rate search)
#
# Unpaced UDP overruns the receiver and drops packets. When an acceptable loss
# target is set, the client increases the aggregate send rate by a percentage
# while loss stays under target. Once it brackets the threshold, it refines the
# range between the highest passing and lowest failing rates.
# ---------------------------------------------------------------------------
DEFAULT_MAX_DURATION_SEC = 120
DEFAULT_ACCEPTABLE_LOSS_PCT = 1.0
UDP_TUNE_PROBE_SEC = 2.0          # paced probe window at the start rate
UDP_TUNE_TRIAL_SEC = 5.0          # continuous per-step measurement window
UDP_TUNE_MIN_TRIAL_DURATION_RATIO = 0.95
UDP_TUNE_SETTLE_SEC = 2.0         # continuous warm-up before measurement
UDP_TUNE_CONTROL_GRACE_SEC = 2.0  # keep workers alive beyond the search controller
UDP_TUNE_BACKOFF_FACTOR = 0.75    # multiplicative decrease on a loss breach
# The ramp is PACED from a fixed start rate (in aggregate Mbps) rather than an
# unpaced line-rate burst. An unpaced probe can momentarily send multi-Gbps,
# which looks like a UDP flood/DDoS and gets dropped or rate-limited by network
# security devices. Starting at UDP_TUNE_START_MBPS and adjusting up/down avoids
# that.
UDP_TUNE_START_MBPS = 250.0       # aggregate Mbps the UDP ramp starts at
UDP_TUNE_MAX_RATE_MBPS = 100_000.0 # aggregate safety ceiling (100 Gbps)
UDP_TUNE_INITIAL_INCREASE_PCT = 25.0  # first increase above the starting rate
UDP_TUNE_INCREASE_GROWTH_FACTOR = 2.0 # grow the increase percentage after each passing trial
UDP_TUNE_MAX_INCREASE_PCT = 100.0     # cap unbounded growth at 2x the prior rate
UDP_TUNE_CONVERGENCE_PCT = 5.0    # stop once passing/failing rates are within this percentage
# Loss alone can't detect a saturated path with a deep buffer: packets pile into
# the buffer (huge latency) instead of being dropped, so loss stays ~0 while the
# offered rate runs far above the real goodput. Treat a higher offered rate that
# fails to raise goodput by at least this fraction as "saturated" and back off,
# so the ramp settles near the true goodput instead of over-driving the buffer.
UDP_TUNE_MIN_GOODPUT_GAIN = 0.05  # required goodput improvement to keep climbing
# TCP moves a byte stream, so the per-stream counters record send()/recv()
# syscalls, not wire packets -- useless as a packets/sec figure. For reporting
# we estimate wire segments from throughput assuming a standard Ethernet MSS
# (1500 MTU - 20 IP - 20 TCP - 12 TCP timestamps).
TCP_MSS_ESTIMATE = 1448

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
TEST_TYPE_FULLBENCH = "fullbench"
TEST_TYPE_MTR = "mtr"
TEST_TYPE_CONNECTIVITY = "connectivity"
TEST_TYPE_PMTUD = "pmtud"
TEST_TYPE_PROTOCOLS = "protocols"
