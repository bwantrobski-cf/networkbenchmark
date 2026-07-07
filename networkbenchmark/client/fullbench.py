"""Full-spectrum benchmark runner for scripting/automation.

Runs a matrix of throughput tests (TCP and/or UDP, upload and/or download)
across a list of stream counts, optionally with UDP loss-target tuning, and:

  * stores each test's structured result locally (JSON + text log), and
  * optionally POSTs each test's JSON result to an HTTP(S) endpoint so an
    external ingestion worker (e.g. a Cloudflare Worker writing to D1) can
    consume it.

This module has no third-party dependencies (uses only the standard library),
so it can run headless anywhere the package is installed.
"""

import json
import socket
import threading
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

from ..common import constants, logging_store
from .engine import ThroughputClientConfig, run_throughput_test


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_int_list(value, field="value"):
    """Parse a comma-separated list of positive ints, e.g. '1,8,16,32'."""
    if isinstance(value, (list, tuple)):
        items = value
    else:
        items = [p for p in str(value).replace(" ", "").split(",") if p != ""]
    out = []
    for p in items:
        try:
            n = int(p)
        except (TypeError, ValueError):
            raise ValueError(f"invalid integer in {field}: {p!r}")
        if n < 1:
            raise ValueError(f"{field} values must be >= 1 (got {n})")
        out.append(n)
    if not out:
        raise ValueError(f"{field} must contain at least one value")
    return out


def parse_csv_choices(value, allowed, field="value"):
    """Parse a comma-separated list constrained to `allowed`."""
    if isinstance(value, (list, tuple)):
        items = [str(v).strip().lower() for v in value]
    else:
        items = [p.strip().lower() for p in str(value).split(",") if p.strip()]
    out = []
    for p in items:
        if p not in allowed:
            raise ValueError(f"invalid {field}: {p!r} (allowed: {', '.join(allowed)})")
        if p not in out:
            out.append(p)
    if not out:
        raise ValueError(f"{field} must contain at least one of: {', '.join(allowed)}")
    return out


def parse_headers(pairs):
    """Parse a list of 'Key: Value' strings into a dict."""
    headers = {}
    for item in pairs or []:
        if ":" not in item:
            raise ValueError(f"invalid header (expected 'Key: Value'): {item!r}")
        key, _, val = item.partition(":")
        headers[key.strip()] = val.strip()
    return headers


# ---------------------------------------------------------------------------
# Config + plan
# ---------------------------------------------------------------------------

class FullBenchConfig:
    def __init__(self, server_ip, control_port=None, transports=("tcp", "udp"),
                 directions=("up", "down"), streams=(1, 8, 16, 32), procs=(1,),
                 duration_sec=constants.DEFAULT_TEST_DURATION_SEC,
                 acceptable_loss_pct=None,
                 udp_max_duration_sec=constants.DEFAULT_MAX_DURATION_SEC,
                 bind="0.0.0.0", start_port=None,
                 export_url=None, export_method="POST", export_headers=None,
                 export_slim=False, export_timeout_sec=15.0, label=None,
                 run_id=None, vm_name=None, cloud=None, environment=None,
                 tool_version=1, test_type="networkbenchmark"):
        self.server_ip = server_ip
        self.control_port = control_port or constants.DEFAULT_CONTROL_PORT
        self.transports = list(transports)
        self.directions = list(directions)
        self.streams = list(streams)
        self.procs = list(procs)
        if len(self.procs) not in (1, len(self.streams)):
            raise ValueError(
                "procs must be a single value or match the number of stream counts "
                f"(got {len(self.procs)} procs for {len(self.streams)} stream counts)")
        self.duration_sec = float(duration_sec)
        self.acceptable_loss_pct = (float(acceptable_loss_pct)
                                    if acceptable_loss_pct is not None else None)
        self.udp_max_duration_sec = float(udp_max_duration_sec)
        self.bind = bind
        self.start_port = start_port
        self.export_url = export_url
        self.export_method = (export_method or "POST").upper()
        self.export_headers = dict(export_headers or {})
        self.export_slim = bool(export_slim)
        self.export_timeout_sec = float(export_timeout_sec)
        self.label = label
        # Identity / ingestion metadata (aligns with worker-results-api schema).
        self.run_id = run_id
        self.vm_name = vm_name or socket.gethostname()
        self.cloud = cloud
        self.environment = environment
        self.tool_version = int(tool_version)
        self.test_type = test_type

    def _procs_for(self, idx):
        return self.procs[idx] if len(self.procs) > 1 else self.procs[0]

    def build_plan(self):
        """Expand the matrix into an ordered list of individual test specs."""
        plan = []
        for transport in self.transports:
            for direction in self.directions:
                for i, streams in enumerate(self.streams):
                    procs = self._procs_for(i)
                    tune = transport == "udp" and self.acceptable_loss_pct is not None
                    plan.append({
                        "transport": transport,
                        "direction": direction,
                        "streams": streams,
                        "procs": procs,
                        "tune": tune,
                    })
        return plan


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def _slim_result(summary):
    """Drop the bulky arrays for lighter export payloads (keeps tuning trials,
    which are small and are the point of a tuning run)."""
    drop = {"interval_series", "per_stream", "source_ports"}
    return {k: v for k, v in summary.items() if k not in drop}


def export_result(url, payload, method="POST", headers=None, timeout_sec=15.0):
    """POST/PUT a JSON payload to an HTTP(S) endpoint. Returns a result dict
    {ok, status, error} and never raises (so a failed export does not abort the
    benchmark)."""
    data = json.dumps(payload, default=str).encode("utf-8")
    hdrs = {"Content-Type": "application/json"}
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            return {"ok": 200 <= int(status) < 300, "status": int(status), "error": None}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status": exc.code, "error": f"HTTP {exc.code}: {exc.reason}"}
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return {"ok": False, "status": None, "error": str(exc)}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _spec_label(spec):
    mode = "tune" if spec.get("tune") else "fixed"
    return f"{spec['transport']}/{spec['direction']}/{spec['streams']}s/{spec['procs']}p/{mode}"


def run_fullbench(cfg: FullBenchConfig, on_progress=None, stop_requested: threading.Event = None,
                  log_base_dir=None):
    """Execute the benchmark matrix sequentially.

    Args:
        cfg: FullBenchConfig.
        on_progress: optional callable(str) for human-readable progress lines.
        stop_requested: optional Event to abort between/after tests.
        log_base_dir: base dir for per-test JSON/text logs (each test is saved
            individually by run_throughput_test).

    Returns a manifest dict describing every test run, and writes a combined
    manifest file locally.
    """
    def log(msg):
        if on_progress:
            on_progress(msg)

    stop_requested = stop_requested or threading.Event()
    benchmark_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    plan = cfg.build_plan()
    total = len(plan)
    log(f"fullbench {benchmark_id}: {total} test(s) queued -> {cfg.server_ip}")

    tests = []
    for idx, spec in enumerate(plan, start=1):
        if stop_requested.is_set():
            log("fullbench: stop requested, aborting remaining tests")
            break

        label = _spec_label(spec)
        log(f"[{idx}/{total}] {label}: starting")

        client_cfg = ThroughputClientConfig(
            server_ip=cfg.server_ip,
            control_port=cfg.control_port,
            transport=spec["transport"],
            direction=spec["direction"],
            streams=spec["streams"],
            duration_sec=cfg.duration_sec,
            start_port=cfg.start_port,
            bind=cfg.bind,
            procs=spec["procs"],
            acceptable_loss_pct=(cfg.acceptable_loss_pct if spec["tune"] else None),
            max_duration_sec=cfg.udp_max_duration_sec,
        )

        # Each individual test carries its own unique recorded_at timestamp.
        record = {
            # Ingestion / identity metadata (worker-results-api conventions).
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "test_type": cfg.test_type,
            "run_id": cfg.run_id,
            "vm_name": cfg.vm_name,
            "cloud": cfg.cloud,
            "environment": cfg.environment,
            "tool_version": cfg.tool_version,
            # Benchmark grouping.
            "benchmark_id": benchmark_id,
            "test_index": idx,
            "test_count": total,
            "label": label,
            "benchmark_label": cfg.label,
            "transport": spec["transport"],
            "direction": spec["direction"],
            "streams": spec["streams"],
            "procs": spec["procs"],
            "tuned": spec["tune"],
        }

        try:
            summary = run_throughput_test(client_cfg, stop_requested=stop_requested,
                                          log_base_dir=log_base_dir)
            record["nb_run_id"] = summary.get("run_id")
            record["ok"] = True
            record["error"] = None
            record["result"] = summary
            record["send_mbps"] = summary.get("send_mbps")
            record["recv_mbps"] = summary.get("recv_mbps")
            record["send_pps"] = summary.get("send_pps")
            record["recv_pps"] = summary.get("recv_pps")
            record["loss_pct"] = summary.get("loss_pct")
            record["log_json"] = summary.get("log_json")
            log(f"[{idx}/{total}] {label}: send={summary.get('send_mbps', 0):.1f} Mbps "
                f"recv={summary.get('recv_mbps', 0):.1f} Mbps "
                f"loss={summary.get('loss_pct')}")
        except Exception as exc:  # noqa: BLE001 -- one test failing must not kill the run
            record["ok"] = False
            record["error"] = str(exc)
            record["result"] = None
            log(f"[{idx}/{total}] {label}: FAILED: {exc}")

        # Export per-test result (if configured).
        if cfg.export_url and record["result"] is not None:
            payload = dict(record)
            payload["result"] = (_slim_result(record["result"]) if cfg.export_slim
                                 else record["result"])
            res = export_result(cfg.export_url, payload, method=cfg.export_method,
                                headers=cfg.export_headers, timeout_sec=cfg.export_timeout_sec)
            record["export"] = res
            if res["ok"]:
                log(f"[{idx}/{total}] {label}: exported (HTTP {res['status']})")
            else:
                log(f"[{idx}/{total}] {label}: export FAILED: {res['error']}")

        tests.append(record)

    manifest = {
        "benchmark_id": benchmark_id,
        "label": cfg.label,
        "server": cfg.server_ip,
        "run_id": cfg.run_id,
        "vm_name": cfg.vm_name,
        "cloud": cfg.cloud,
        "environment": cfg.environment,
        "tool_version": cfg.tool_version,
        "test_type": cfg.test_type,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "transports": cfg.transports,
            "directions": cfg.directions,
            "streams": cfg.streams,
            "procs": cfg.procs,
            "duration_sec": cfg.duration_sec,
            "acceptable_loss_pct": cfg.acceptable_loss_pct,
            "udp_max_duration_sec": cfg.udp_max_duration_sec,
            "export_url": cfg.export_url,
        },
        "test_count": total,
        "completed": len(tests),
        "tests": tests,
    }

    text_lines = [f"fullbench {benchmark_id} -> {cfg.server_ip}", ""]
    for t in tests:
        if t["ok"]:
            text_lines.append(f"{t['label']}: send={t.get('send_mbps')} Mbps "
                              f"recv={t.get('recv_mbps')} Mbps loss={t.get('loss_pct')}")
        else:
            text_lines.append(f"{t['label']}: FAILED: {t.get('error')}")
    json_path, log_path = logging_store.save_result(
        constants.TEST_TYPE_FULLBENCH, manifest, text_lines, base_dir=log_base_dir)
    manifest["manifest_json"] = json_path
    manifest["manifest_log"] = log_path
    log(f"fullbench manifest: {json_path}")
    return manifest
