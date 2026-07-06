"""Control-channel latency probing.

The client periodically sends a small `probe` message over the (always
present) TCP control connection; the peer replies immediately with
`probe_ack` echoing the original send time. Because the control connection
traverses the same path as the data streams, RTT measured this way still
reflects shared-link queueing/bufferbloat during the running phase, while
giving an "unloaded" baseline during the calibration phase.
"""

import time

from ..common import constants


def prober_loop(control, stop_event, interval_sec: float = constants.PROBE_INTERVAL_SEC):
    """Runs in a background thread on the probing side (the client)."""
    while not stop_event.is_set():
        try:
            control.send("probe", t_send=time.time())
        except OSError:
            break
        stop_event.wait(interval_sec)


def handle_probe_message(control, msg: dict):
    """Call from the peer's (server's) control-message dispatch loop."""
    control.send("probe_ack", t_send=msg.get("t_send"))


def rtt_ms_from_ack(msg: dict):
    t_send = msg.get("t_send")
    if t_send is None:
        return None
    return (time.time() - t_send) * 1000.0
