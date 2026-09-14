"""Shared server runtime state: config, capabilities, and the single
active throughput session (only one throughput test runs at a time)."""

import threading
import time


class ServerConfig:
    def __init__(self, bind="0.0.0.0", control_port=None, udp_port=None, tcp_port=None,
                 verbose=False, log_dir=None):
        from ..common import constants
        self.bind = bind
        self.control_port = control_port or constants.DEFAULT_CONTROL_PORT
        self.udp_port = udp_port or constants.DEFAULT_UDP_PORT
        self.tcp_port = tcp_port or constants.DEFAULT_TCP_PORT
        self.verbose = verbose
        self.log_dir = log_dir


class ServerContext:
    def __init__(self, config: ServerConfig, caps: dict, on_log=None):
        self.config = config
        self.caps = caps
        self._lock = threading.Lock()
        self.active_session = None
        self.stop_event = threading.Event()
        self._on_log = on_log

    def set_session(self, session):
        with self._lock:
            if self.active_session is not None:
                return False
            self.active_session = session
            return True

    def clear_session(self, run_id):
        with self._lock:
            if self.active_session is not None and self.active_session.run_id == run_id:
                self.active_session = None

    def get_session(self):
        with self._lock:
            return self.active_session

    def log(self, msg: str):
        line = f"[{time.strftime('%H:%M:%S')}] [server] {msg}"
        if self._on_log:
            self._on_log(line)
        else:
            print(line, flush=True)
