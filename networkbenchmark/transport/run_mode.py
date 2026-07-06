"""Test lifecycle phase tracking: calibration -> running -> stopped.

Adapted from bbperf's run_mode_manager_class.py. During "calibration" only
the control-channel latency probe runs (no data streams), establishing the
unloaded RTT. "running" begins once the sending side is signalled to start
its data stream(s); "stopped" ends the test.
"""

import threading


class RunPhase:
    CALIBRATION = "calibration"
    RUNNING = "running"
    STOPPED = "stopped"

    def __init__(self):
        self._phase = self.CALIBRATION
        self._lock = threading.Lock()
        self.stop_event = threading.Event()

    @property
    def phase(self) -> str:
        with self._lock:
            return self._phase

    def start_running(self):
        with self._lock:
            self._phase = self.RUNNING

    def stop(self):
        with self._lock:
            self._phase = self.STOPPED
        self.stop_event.set()

    def is_stopped(self) -> bool:
        return self.stop_event.is_set()
