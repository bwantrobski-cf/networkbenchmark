"""Best-effort Linux host telemetry and bottleneck classification.

All snapshots and deltas are JSON-friendly dictionaries. Sampling functions
return ``available`` and ``reason`` metadata instead of raising when procfs is
unsupported, missing, unreadable, or malformed. The parsers and classifier do
not perform I/O and are useful with captured procfs text.
"""

import math
import os
import platform
import time
from collections.abc import Mapping


TARGET_ATTAINMENT_RATIO = 0.95
CPU_BUSY_THRESHOLD_PCT = 90.0
CPU_SOFTIRQ_THRESHOLD_PCT = 25.0

_CPU_FIELDS = (
    "user",
    "nice",
    "system",
    "idle",
    "iowait",
    "irq",
    "softirq",
    "steal",
    "guest",
    "guest_nice",
)

__all__ = [
    "CPU_BUSY_THRESHOLD_PCT",
    "CPU_SOFTIRQ_THRESHOLD_PCT",
    "TARGET_ATTAINMENT_RATIO",
    "classify_bottleneck",
    "diff_endpoint",
    "diff_host_cpu",
    "diff_pid_cpu",
    "diff_udp_counters",
    "normalize_ip_family",
    "parse_proc_net_snmp",
    "parse_proc_net_snmp6",
    "parse_proc_pid_stat",
    "parse_proc_stat",
    "snapshot_endpoint",
    "snapshot_host_cpu",
    "snapshot_pid_cpu",
    "snapshot_udp_counters",
]


def _capture_time(value=None):
    if value is not None:
        number = _finite_number(value)
        if number is not None:
            return number
    try:
        return time.monotonic()
    except Exception:
        return None


def _finite_number(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _nonnegative_number(value):
    number = _finite_number(value)
    if number is None or number < 0:
        return None
    return number


def _source_path(proc_root, *parts):
    try:
        return os.path.join(os.fspath(proc_root), *parts)
    except Exception:
        return None


def _is_linux(platform_name=None):
    try:
        name = platform.system() if platform_name is None else platform_name
        return str(name).lower() == "linux"
    except Exception:
        return False


def _read_text(path):
    if path is None:
        return None, "invalid_path", None
    try:
        with open(path, "r", encoding="ascii", errors="replace") as handle:
            return handle.read(), None, None
    except FileNotFoundError as exc:
        return None, "not_found", str(exc)
    except PermissionError as exc:
        return None, "permission_denied", str(exc)
    except OSError as exc:
        return None, "unavailable", str(exc)
    except Exception as exc:
        return None, "unavailable", str(exc)


def _elapsed_seconds(before, after):
    start = _finite_number(before.get("captured_at")) if isinstance(before, Mapping) else None
    end = _finite_number(after.get("captured_at")) if isinstance(after, Mapping) else None
    if start is None or end is None or end <= start:
        return None
    return end - start


def _counter(value):
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number >= 0 else None


def _cpu_counters(values):
    if len(values) < 4:
        return None
    counters = []
    for value in values[:len(_CPU_FIELDS)]:
        parsed = _counter(value)
        if parsed is None:
            return None
        counters.append(parsed)
    counters.extend([0] * (len(_CPU_FIELDS) - len(counters)))
    result = dict(zip(_CPU_FIELDS, counters))

    # guest and guest_nice are already included in user and nice by Linux.
    result["total"] = sum(result[name] for name in _CPU_FIELDS[:8])
    result["busy"] = max(0, result["total"] - result["idle"] - result["iowait"])
    return result


def parse_proc_stat(text):
    """Parse CPU rows from ``/proc/stat`` without raising.

    Returns ``{"host": counters_or_none, "cores": {core_id: counters}}``.
    Counter values are scheduler ticks; ``busy`` excludes idle and iowait.
    """
    result = {"host": None, "cores": {}}
    try:
        for raw_line in text.splitlines():
            fields = raw_line.split()
            if not fields:
                continue
            label = fields[0]
            if label != "cpu" and not (label.startswith("cpu") and label[3:].isdigit()):
                continue
            counters = _cpu_counters(fields[1:])
            if counters is None:
                continue
            if label == "cpu":
                result["host"] = counters
            else:
                result["cores"][label[3:]] = counters
    except Exception:
        return {"host": None, "cores": {}}
    return result


def snapshot_host_cpu(proc_root="/proc", platform_name=None, captured_at=None):
    """Return a best-effort Linux host CPU counter snapshot."""
    timestamp = _capture_time(captured_at)
    source = _source_path(proc_root, "stat")
    empty = {
        "available": False,
        "reason": "unsupported_platform",
        "error": None,
        "source": source,
        "captured_at": timestamp,
        "host": None,
        "cores": {},
    }
    try:
        if not _is_linux(platform_name):
            return empty
        text, reason, error = _read_text(source)
        if text is None:
            empty["reason"] = reason
            empty["error"] = error
            return empty
        parsed = parse_proc_stat(text)
        if parsed["host"] is None:
            empty["reason"] = "invalid_data"
            return empty
        empty.update({
            "available": True,
            "reason": None,
            "host": parsed["host"],
            "cores": parsed["cores"],
        })
        return empty
    except Exception as exc:
        empty["reason"] = "unavailable"
        empty["error"] = str(exc)
        return empty


def _cpu_delta(before, after):
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        return None
    deltas = {}
    for name in _CPU_FIELDS[:8]:
        start = _counter(before.get(name))
        end = _counter(after.get(name))
        if start is None or end is None:
            return None
        deltas[name] = max(0, end - start)

    start_total = _counter(before.get("total"))
    end_total = _counter(after.get("total"))
    if start_total is None or end_total is None or end_total <= start_total:
        return None
    total = end_total - start_total
    user = min(total, deltas["user"] + deltas["nice"])
    system = min(total, deltas["system"] + deltas["irq"])
    softirq = min(total, deltas["softirq"])
    busy = min(total, sum(deltas[name] for name in (
        "user", "nice", "system", "irq", "softirq", "steal")))

    def percentage(value):
        return value * 100.0 / total

    return {
        "user_pct": percentage(user),
        "system_pct": percentage(system),
        "softirq_pct": percentage(softirq),
        "busy_pct": percentage(busy),
        "user_ticks": user,
        "system_ticks": system,
        "softirq_ticks": softirq,
        "busy_ticks": busy,
        "total_ticks": total,
    }


def _core_sort_key(core_id):
    try:
        return 0, int(core_id)
    except (TypeError, ValueError, OverflowError):
        return 1, str(core_id)


def diff_host_cpu(before, after):
    """Return whole-host and per-core percentages between CPU snapshots."""
    result = {
        "available": False,
        "reason": "snapshot_unavailable",
        "elapsed_sec": _elapsed_seconds(before, after),
        "host": None,
        "cores": {},
    }
    try:
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            result["reason"] = "invalid_snapshot"
            return result
        if before.get("available") is False or after.get("available") is False:
            return result
        host = _cpu_delta(before.get("host"), after.get("host"))
        if host is None:
            result["reason"] = "invalid_delta"
            return result

        before_cores = before.get("cores")
        after_cores = after.get("cores")
        if not isinstance(before_cores, Mapping):
            before_cores = {}
        if not isinstance(after_cores, Mapping):
            after_cores = {}
        cores = {}
        common = set(before_cores).intersection(after_cores)
        for core_id in sorted(common, key=_core_sort_key):
            delta = _cpu_delta(before_cores.get(core_id), after_cores.get(core_id))
            if delta is not None:
                cores[str(core_id)] = delta

        result.update({
            "available": True,
            "reason": None,
            "host": host,
            "cores": cores,
        })
        return result
    except Exception:
        result["reason"] = "invalid_snapshot"
        return result


def parse_proc_net_snmp(text):
    """Return the IPv4 ``Udp`` counter row from ``/proc/net/snmp``."""
    try:
        rows = []
        for raw_line in text.splitlines():
            fields = raw_line.split()
            if fields and fields[0] == "Udp:":
                rows.append(fields[1:])
        for index in range(len(rows) - 1):
            names = rows[index]
            values = rows[index + 1]
            if len(names) != len(values) or not names:
                continue
            parsed = [_counter(value) for value in values]
            if any(value is None for value in parsed):
                continue
            return dict(zip(names, parsed))
    except Exception:
        return None
    return None


def parse_proc_net_snmp6(text):
    """Return normalized IPv6 ``Udp6`` counters from ``/proc/net/snmp6``."""
    counters = {}
    try:
        for raw_line in text.splitlines():
            fields = raw_line.split()
            if len(fields) != 2 or not fields[0].startswith("Udp6"):
                continue
            name = fields[0][len("Udp6"):]
            value = _counter(fields[1])
            if name and value is not None:
                counters[name] = value
    except Exception:
        return None
    return counters or None


def _udp_family_snapshot(path, parser):
    text, reason, error = _read_text(path)
    family = {
        "available": False,
        "reason": reason,
        "error": error,
        "source": path,
        "counters": None,
    }
    if text is None:
        return family
    counters = parser(text)
    if counters is None:
        family["reason"] = "invalid_data"
        return family
    family.update({"available": True, "reason": None, "counters": counters})
    return family


def _sum_counters(counter_sets):
    names = set()
    for counters in counter_sets:
        if isinstance(counters, Mapping):
            names.update(counters)
    if not names:
        return None
    totals = {}
    for name in sorted(names):
        values = []
        for counters in counter_sets:
            if not isinstance(counters, Mapping):
                continue
            value = _counter(counters.get(name))
            if value is not None:
                values.append(value)
        totals[name] = sum(values) if values else None
    return totals


def _active_udp_family(ipv4, ipv6):
    """Infer the active family only when its datagram delta is dominant."""
    activity = {}
    for name, family in (("ipv4", ipv4), ("ipv6", ipv6)):
        counters = _mapping(_mapping(family).get("counters"))
        inbound = _metric(counters, "InDatagrams") or 0
        outbound = _metric(counters, "OutDatagrams") or 0
        activity[name] = inbound + outbound
    if activity["ipv4"] == activity["ipv6"]:
        return None
    selected = max(activity, key=activity.get)
    return selected if activity[selected] > 0 else None


def snapshot_udp_counters(proc_root="/proc", platform_name=None, captured_at=None):
    """Return host-wide IPv4 and IPv6 UDP counters from procfs."""
    timestamp = _capture_time(captured_at)
    ipv4_path = _source_path(proc_root, "net", "snmp")
    ipv6_path = _source_path(proc_root, "net", "snmp6")
    unsupported = {
        "available": False,
        "complete": False,
        "reason": "unsupported_platform",
        "captured_at": timestamp,
        "ipv4": {
            "available": False, "reason": "unsupported_platform", "error": None,
            "source": ipv4_path, "counters": None,
        },
        "ipv6": {
            "available": False, "reason": "unsupported_platform", "error": None,
            "source": ipv6_path, "counters": None,
        },
        "total": None,
    }
    try:
        if not _is_linux(platform_name):
            return unsupported
        ipv4 = _udp_family_snapshot(ipv4_path, parse_proc_net_snmp)
        ipv6 = _udp_family_snapshot(ipv6_path, parse_proc_net_snmp6)
        available = ipv4["available"] or ipv6["available"]
        complete = ipv4["available"] and ipv6["available"]
        total = _sum_counters([
            ipv4.get("counters") if ipv4["available"] else None,
            ipv6.get("counters") if ipv6["available"] else None,
        ])
        return {
            "available": available,
            "complete": complete,
            "reason": None if complete else ("partial" if available else "unavailable"),
            "captured_at": timestamp,
            "ipv4": ipv4,
            "ipv6": ipv6,
            "total": total,
        }
    except Exception:
        unsupported["reason"] = "unavailable"
        return unsupported


def _diff_udp_family(before, after):
    result = {
        "available": False,
        "reason": "snapshot_unavailable",
        "counters": None,
        "reset_fields": [],
    }
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        result["reason"] = "invalid_snapshot"
        return result
    if not before.get("available") or not after.get("available"):
        return result
    starts = before.get("counters")
    ends = after.get("counters")
    if not isinstance(starts, Mapping) or not isinstance(ends, Mapping):
        result["reason"] = "invalid_snapshot"
        return result

    counters = {}
    reset_fields = []
    for name in sorted(set(starts).union(ends)):
        start = _counter(starts.get(name))
        end = _counter(ends.get(name))
        if start is None or end is None:
            counters[name] = None
        elif end < start:
            counters[name] = None
            reset_fields.append(name)
        else:
            counters[name] = end - start
    available = any(value is not None for value in counters.values())
    result.update({
        "available": available,
        "reason": ("partial" if reset_fields and available else
                   "counter_reset" if reset_fields else
                   None if available else "invalid_delta"),
        "counters": counters or None,
        "reset_fields": reset_fields,
    })
    return result


def diff_udp_counters(before, after):
    """Return nonnegative UDP counter deltas for matching snapshots."""
    result = {
        "available": False,
        "complete": False,
        "reason": "snapshot_unavailable",
        "elapsed_sec": _elapsed_seconds(before, after),
        "active_family": None,
        "ipv4": _diff_udp_family(None, None),
        "ipv6": _diff_udp_family(None, None),
        "total": None,
    }
    try:
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            result["reason"] = "invalid_snapshot"
            return result
        ipv4 = _diff_udp_family(before.get("ipv4"), after.get("ipv4"))
        ipv6 = _diff_udp_family(before.get("ipv6"), after.get("ipv6"))
        available = ipv4["available"] or ipv6["available"]
        complete = (ipv4["available"] and ipv6["available"] and
                    not ipv4["reset_fields"] and not ipv6["reset_fields"])
        total = _sum_counters([
            ipv4.get("counters") if ipv4["available"] else None,
            ipv6.get("counters") if ipv6["available"] else None,
        ])
        result.update({
            "available": available,
            "complete": complete,
            "reason": None if complete else ("partial" if available else "snapshot_unavailable"),
            "active_family": _active_udp_family(ipv4, ipv6),
            "ipv4": ipv4,
            "ipv6": ipv6,
            "total": total,
        })
        return result
    except Exception:
        result["reason"] = "invalid_snapshot"
        return result


def parse_proc_pid_stat(text):
    """Parse selected fields from ``/proc/<pid>/stat`` without raising.

    The command name is delimited with the first ``(`` and final ``)`` so
    embedded spaces and parentheses do not shift the numeric field indexes.
    """
    try:
        line = text.strip()
        open_paren = line.find("(")
        close_paren = line.rfind(")")
        if open_paren <= 0 or close_paren <= open_paren:
            return None
        pid = _counter(line[:open_paren].strip())
        remainder = line[close_paren + 1:].split()
        if pid is None or len(remainder) < 20:
            return None
        values = {
            "user_ticks": _counter(remainder[11]),
            "system_ticks": _counter(remainder[12]),
            "children_user_ticks": _counter(remainder[13]),
            "children_system_ticks": _counter(remainder[14]),
            "starttime_ticks": _counter(remainder[19]),
        }
        if any(value is None for value in values.values()):
            return None
        return {
            "pid": pid,
            "comm": line[open_paren + 1:close_paren],
            "state": remainder[0],
            **values,
            "cpu_ticks": values["user_ticks"] + values["system_ticks"],
        }
    except Exception:
        return None


def _pid_snapshot_result(pid, source, captured_at, reason):
    return {
        "available": False,
        "reason": reason,
        "error": None,
        "source": source,
        "captured_at": captured_at,
        "pid": pid,
        "comm": None,
        "state": None,
        "starttime_ticks": None,
        "clock_ticks_per_sec": None,
        "user_ticks": None,
        "system_ticks": None,
        "children_user_ticks": None,
        "children_system_ticks": None,
        "cpu_ticks": None,
    }


def _normalize_pid(pid):
    if isinstance(pid, bool) or isinstance(pid, float):
        return None
    try:
        value = int(pid)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if value > 0 else None


def _clock_ticks_per_second():
    try:
        value = int(os.sysconf("SC_CLK_TCK"))
        return value if value > 0 else None
    except Exception:
        return None


def snapshot_pid_cpu(pid=None, proc_root="/proc", platform_name=None, captured_at=None):
    """Return CPU counters for the current or explicitly selected PID."""
    timestamp = _capture_time(captured_at)
    try:
        requested_pid = os.getpid() if pid is None else pid
    except Exception:
        requested_pid = None
    normalized_pid = _normalize_pid(requested_pid)
    source = _source_path(proc_root, str(normalized_pid), "stat") if normalized_pid else None
    result = _pid_snapshot_result(normalized_pid, source, timestamp, "unsupported_platform")
    try:
        if normalized_pid is None:
            result["reason"] = "invalid_pid"
            return result
        if not _is_linux(platform_name):
            return result
        text, reason, error = _read_text(source)
        if text is None:
            result["reason"] = reason
            result["error"] = error
            return result
        parsed = parse_proc_pid_stat(text)
        if parsed is None:
            result["reason"] = "invalid_data"
            return result
        if parsed["pid"] != normalized_pid:
            result["reason"] = "pid_mismatch"
            return result
        result.update(parsed)
        result.update({
            "available": True,
            "reason": None,
            "clock_ticks_per_sec": _clock_ticks_per_second(),
        })
        return result
    except Exception as exc:
        result["reason"] = "unavailable"
        result["error"] = str(exc)
        return result


def _empty_pid_delta(reason="snapshot_unavailable"):
    return {
        "available": False,
        "complete": False,
        "reason": reason,
        "pid": None,
        "comm": None,
        "starttime_ticks": None,
        "elapsed_sec": None,
        "user_ticks": None,
        "system_ticks": None,
        "children_user_ticks": None,
        "children_system_ticks": None,
        "cpu_ticks": None,
        "user_pct": None,
        "system_pct": None,
        "cpu_pct": None,
    }


def diff_pid_cpu(before, after):
    """Return PID CPU deltas only when PID and process starttime both match."""
    result = _empty_pid_delta()
    try:
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            result["reason"] = "invalid_snapshot"
            return result
        if not before.get("available") or not after.get("available"):
            return result
        pid = _counter(before.get("pid"))
        if pid is None or pid != _counter(after.get("pid")):
            result["reason"] = "pid_changed"
            return result
        starttime = _counter(before.get("starttime_ticks"))
        if starttime is None or starttime != _counter(after.get("starttime_ticks")):
            result["reason"] = "pid_reused"
            return result

        names = ("user_ticks", "system_ticks", "children_user_ticks", "children_system_ticks")
        deltas = {}
        for name in names:
            start = _counter(before.get(name))
            end = _counter(after.get(name))
            if start is None or end is None or end < start:
                result["reason"] = "counter_reset"
                return result
            deltas[name] = end - start
        elapsed = _elapsed_seconds(before, after)
        before_hz = _counter(before.get("clock_ticks_per_sec"))
        after_hz = _counter(after.get("clock_ticks_per_sec"))
        clock_hz = after_hz or before_hz
        if before_hz and after_hz and before_hz != after_hz:
            clock_hz = None

        user_pct = None
        system_pct = None
        if elapsed is not None and clock_hz:
            user_pct = deltas["user_ticks"] * 100.0 / (clock_hz * elapsed)
            system_pct = deltas["system_ticks"] * 100.0 / (clock_hz * elapsed)
        complete = user_pct is not None and system_pct is not None
        result.update({
            "available": True,
            "complete": complete,
            "reason": None if complete else "elapsed_unavailable",
            "pid": pid,
            "comm": after.get("comm"),
            "starttime_ticks": starttime,
            "elapsed_sec": elapsed,
            **deltas,
            "cpu_ticks": deltas["user_ticks"] + deltas["system_ticks"],
            "user_pct": user_pct,
            "system_pct": system_pct,
            "cpu_pct": user_pct + system_pct if complete else None,
        })
        return result
    except Exception:
        result["reason"] = "invalid_snapshot"
        return result


def _component_complete(component):
    return (isinstance(component, Mapping) and component.get("available") is True and
            component.get("complete", True) is not False)


def _endpoint_status(components):
    availability = [isinstance(item, Mapping) and item.get("available") is True
                    for item in components]
    available = any(availability)
    complete = all(_component_complete(item) for item in components)
    if complete:
        reason = None
    elif available:
        reason = "partial"
    else:
        reasons = {item.get("reason") for item in components if isinstance(item, Mapping)}
        reason = "unsupported_platform" if reasons == {"unsupported_platform"} else "unavailable"
    return available, complete, reason


def snapshot_endpoint(pid=None, proc_root="/proc", platform_name=None, captured_at=None,
                      pids=None, tested_family=None):
    """Take a generic endpoint snapshot of host CPU, PID CPU, and UDP data."""
    timestamp = _capture_time(captured_at)
    try:
        host_cpu = snapshot_host_cpu(proc_root, platform_name, timestamp)
        requested_pids = []
        for candidate in ([pid] if pids is None else pids):
            normalized = _normalize_pid(candidate)
            if normalized is not None and normalized not in requested_pids:
                requested_pids.append(normalized)
        if not requested_pids:
            requested_pids.append(_normalize_pid(pid) or os.getpid())
        process_cpus = [
            snapshot_pid_cpu(candidate, proc_root, platform_name, timestamp)
            for candidate in requested_pids
        ]
        process_cpu = process_cpus[0]
        udp = snapshot_udp_counters(proc_root, platform_name, timestamp)
        available, complete, reason = _endpoint_status((host_cpu, process_cpu, udp))
        return {
            "available": available,
            "complete": complete,
            "reason": reason,
            "captured_at": timestamp,
            "tested_family": _normalize_ip_family(tested_family),
            "pid": process_cpu.get("pid"),
            "host_cpu": host_cpu,
            "process_cpu": process_cpu,
            "process_cpus": process_cpus,
            "udp": udp,
        }
    except Exception:
        return {
            "available": False,
            "complete": False,
            "reason": "unavailable",
            "captured_at": timestamp,
            "tested_family": _normalize_ip_family(tested_family),
            "pid": _normalize_pid(pid),
            "host_cpu": None,
            "process_cpu": None,
            "process_cpus": [],
            "udp": None,
        }


def diff_endpoint(before, after):
    """Diff two endpoint snapshots, preserving unavailable components."""
    elapsed = _elapsed_seconds(before, after)
    try:
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            raise TypeError("endpoint snapshots must be mappings")
        host_cpu = diff_host_cpu(before.get("host_cpu"), after.get("host_cpu"))
        before_processes = before.get("process_cpus")
        after_processes = after.get("process_cpus")
        if not isinstance(before_processes, list):
            before_processes = [before.get("process_cpu")]
        if not isinstance(after_processes, list):
            after_processes = [after.get("process_cpu")]
        before_by_pid = {
            item.get("pid"): item for item in before_processes
            if isinstance(item, Mapping) and item.get("pid") is not None
        }
        after_by_pid = {
            item.get("pid"): item for item in after_processes
            if isinstance(item, Mapping) and item.get("pid") is not None
        }
        process_deltas = [
            diff_pid_cpu(before_by_pid[candidate], after_by_pid[candidate])
            for candidate in sorted(set(before_by_pid).intersection(after_by_pid))
        ]
        available_processes = [item for item in process_deltas if item.get("available")]
        if available_processes:
            percentages = ("user_pct", "system_pct", "cpu_pct")
            process_cpu = {
                "available": True,
                "complete": (len(available_processes) == len(process_deltas)
                             and all(item.get("complete") for item in available_processes)),
                "reason": None,
                "pid": (available_processes[0].get("pid")
                        if len(available_processes) == 1 else None),
                "pids": [item.get("pid") for item in available_processes],
                "process_count": len(available_processes),
                "elapsed_sec": elapsed,
                "user_ticks": sum(item.get("user_ticks", 0) or 0
                                  for item in available_processes),
                "system_ticks": sum(item.get("system_ticks", 0) or 0
                                    for item in available_processes),
                "cpu_ticks": sum(item.get("cpu_ticks", 0) or 0
                                 for item in available_processes),
            }
            for name in percentages:
                values = [item.get(name) for item in available_processes]
                complete_values = all(value is not None for value in values)
                aggregate = sum(values) if complete_values else None
                peak = max(values) if complete_values else None
                process_cpu[name] = peak
                process_cpu["max_" + name] = peak
                process_cpu["aggregate_" + name] = aggregate
        else:
            process_cpu = _empty_pid_delta()
        udp = diff_udp_counters(before.get("udp"), after.get("udp"))
        available, complete, reason = _endpoint_status((host_cpu, process_cpu, udp))
        tested_family = (_endpoint_tested_family(after)
                         or _endpoint_tested_family(before))
        return {
            "available": available,
            "complete": complete,
            "reason": reason,
            "elapsed_sec": elapsed,
            "tested_family": tested_family,
            "pid": process_cpu.get("pid"),
            "host_cpu": host_cpu,
            "process_cpu": process_cpu,
            "process_cpus": process_deltas,
            "udp": udp,
        }
    except Exception:
        return {
            "available": False,
            "complete": False,
            "reason": "invalid_snapshot",
            "elapsed_sec": elapsed,
            "tested_family": None,
            "pid": None,
            "host_cpu": None,
            "process_cpu": None,
            "process_cpus": [],
            "udp": None,
        }


def _mapping(value):
    return value if isinstance(value, Mapping) else {}


def _metric(mapping, name):
    return _nonnegative_number(mapping.get(name)) if isinstance(mapping, Mapping) else None


def _normalize_ip_family(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        if value == 2:
            return "ipv4"
        if value in (10, 23, 30):
            return "ipv6"
    try:
        normalized = str(value).strip().lower().replace("-", "").replace("_", "")
    except Exception:
        return None
    if normalized in ("4", "ip4", "ipv4", "inet", "afinet"):
        return "ipv4"
    if normalized in ("6", "ip6", "ipv6", "inet6", "afinet6"):
        return "ipv6"
    return None


def normalize_ip_family(value):
    """Return ``ipv4``/``ipv6`` for supported family representations."""
    return _normalize_ip_family(value)


def _endpoint_tested_family(endpoint, explicit=None):
    family = _normalize_ip_family(explicit)
    if family is not None:
        return family
    endpoint = _mapping(endpoint)
    containers = (
        endpoint,
        _mapping(endpoint.get("udp")),
        _mapping(endpoint.get("topology")),
        _mapping(endpoint.get("params")),
    )
    for container in containers:
        for name in ("tested_family", "ip_family", "family", "ip_version",
                     "active_family"):
            family = _normalize_ip_family(container.get(name))
            if family is not None:
                return family
    return None


def _endpoint_udp_counter(endpoint, name, tested_family=None):
    udp = _mapping(_mapping(endpoint).get("udp"))
    family = _endpoint_tested_family(endpoint, tested_family)
    if family is not None and family in udp:
        family_delta = _mapping(udp.get(family))
        if family_delta.get("available") is False:
            return None
        counters = family_delta.get("counters")
        if isinstance(counters, Mapping):
            return _metric(counters, name)
        return _metric(family_delta, name)
    return _metric(_mapping(udp.get("total")), name)


def _endpoint_is_available(endpoint):
    endpoint = _mapping(endpoint)
    if endpoint.get("available") is True:
        return True
    if endpoint.get("available") is False:
        return False
    return any(_mapping(endpoint.get(name)).get("available") is True
               for name in ("host_cpu", "process_cpu", "udp"))


def _endpoint_is_complete(endpoint):
    endpoint = _mapping(endpoint)
    if endpoint.get("complete") is not True:
        return False
    return all(_mapping(endpoint.get(name)).get("available") is True
               for name in ("host_cpu", "process_cpu", "udp"))


def _endpoint_cpu_evidence(endpoint, scope):
    endpoint = _mapping(endpoint)
    evidence = []
    strong = False
    process_cpu = _mapping(endpoint.get("process_cpu"))
    process_values = []
    process_cpus = endpoint.get("process_cpus")
    if isinstance(process_cpus, list):
        process_values = [
            value for value in (_metric(item, "cpu_pct") for item in process_cpus)
            if value is not None
        ]
    process_pct = max(process_values) if process_values else _metric(
        process_cpu, "max_cpu_pct")
    pids = process_cpu.get("pids")
    if process_pct is None and not (isinstance(pids, list) and len(pids) > 1):
        process_pct = _metric(process_cpu, "cpu_pct")
    if process_pct is not None and process_pct >= CPU_BUSY_THRESHOLD_PCT:
        evidence.append(scope + "_process_cpu_high")
        strong = True

    host_cpu = _mapping(endpoint.get("host_cpu"))
    host = _mapping(host_cpu.get("host"))
    host_busy = _metric(host, "busy_pct")
    if host_busy is not None and host_busy >= CPU_BUSY_THRESHOLD_PCT:
        evidence.append(scope + "_host_cpu_high")

    core_busy_high = False
    softirq_high = False
    host_softirq = _metric(host, "softirq_pct")
    if host_softirq is not None and host_softirq >= CPU_SOFTIRQ_THRESHOLD_PCT:
        softirq_high = True
    cores = host_cpu.get("cores")
    if isinstance(cores, Mapping):
        for core in cores.values():
            busy = _metric(core, "busy_pct")
            softirq = _metric(core, "softirq_pct")
            if busy is not None and busy >= CPU_BUSY_THRESHOLD_PCT:
                core_busy_high = True
            if softirq is not None and softirq >= CPU_SOFTIRQ_THRESHOLD_PCT:
                softirq_high = True
    if core_busy_high:
        evidence.append(scope + "_core_cpu_high")
    if softirq_high:
        evidence.append(scope + "_softirq_high")
    return evidence, strong


def _socket_overflow_values(socket_overflow_deltas):
    if isinstance(socket_overflow_deltas, Mapping):
        return (
            _nonnegative_number(socket_overflow_deltas.get("sender")),
            _nonnegative_number(socket_overflow_deltas.get("receiver")),
        )
    # SO_RXQ_OVFL is a receive-side counter, so a scalar belongs to receiver.
    return None, _nonnegative_number(socket_overflow_deltas)


def classify_bottleneck(target_mbps, offered_mbps, sender_endpoint_delta=None,
                        receiver_endpoint_delta=None, socket_overflow_deltas=None,
                        loss_pct=None, tested_family=None):
    """Classify sender attainment separately from receiver loss causality.

    Top-level ``primary``/``scope`` describe why the sender missed its offered
    rate target. ``loss_causality`` independently describes receiver/path loss.
    Generic procfs UDP errors are causal only when ``loss_pct`` confirms loss.
    ``tested_family`` accepts IPv4/IPv6 names or AF_INET/AF_INET6 numeric values.
    """
    target = _nonnegative_number(target_mbps)
    offered = _nonnegative_number(offered_mbps)
    benchmark_loss = _nonnegative_number(loss_pct)
    selected_family = _normalize_ip_family(tested_family)
    sender_family = _endpoint_tested_family(sender_endpoint_delta, selected_family)
    receiver_family = _endpoint_tested_family(receiver_endpoint_delta, selected_family)
    if selected_family is None:
        if sender_family == receiver_family:
            selected_family = sender_family
        elif sender_family is None:
            selected_family = receiver_family
        elif receiver_family is None:
            selected_family = sender_family

    result = {
        "primary": "unknown",
        "scope": "unknown",
        "confidence": "low",
        "evidence": [],
        "target_mbps": target,
        "offered_mbps": offered,
        "target_attained": None,
        "target_attainment_pct": None,
        "target_threshold_pct": TARGET_ATTAINMENT_RATIO * 100.0,
        "tested_family": selected_family,
        "loss_pct": benchmark_loss,
        "sender_attainment": {
            "primary": "unknown",
            "scope": "unknown",
            "confidence": "low",
            "evidence": [],
            "target_attained": None,
        },
        "loss_causality": {
            "primary": "unknown",
            "scope": "unknown",
            "confidence": "low",
            "evidence": [],
            "loss_pct": benchmark_loss,
            "loss_observed": None if benchmark_loss is None else benchmark_loss > 0,
        },
    }
    try:
        if target is None or target <= 0 or offered is None:
            result["evidence"].append("throughput_unavailable")
            result["sender_attainment"]["evidence"] = ["throughput_unavailable"]
            return result

        ratio = offered / target
        target_attained = ratio >= TARGET_ATTAINMENT_RATIO
        result["target_attained"] = target_attained
        result["sender_attainment"]["target_attained"] = target_attained
        if math.isfinite(ratio * 100.0):
            result["target_attainment_pct"] = ratio * 100.0
        target_evidence = "target_attained" if target_attained else "target_missed"

        sender_socket, receiver_socket = _socket_overflow_values(socket_overflow_deltas)
        sender_udp_send = _endpoint_udp_counter(
            sender_endpoint_delta, "SndbufErrors", selected_family)
        receiver_udp_buffer = _endpoint_udp_counter(
            receiver_endpoint_delta, "RcvbufErrors", selected_family)
        receiver_udp_errors = _endpoint_udp_counter(
            receiver_endpoint_delta, "InErrors", selected_family)

        sender_evidence = [target_evidence]
        loss_evidence = []
        if benchmark_loss is None:
            loss_evidence.append("benchmark_loss_unavailable")
        elif benchmark_loss > 0:
            loss_evidence.append("benchmark_loss_observed")
        else:
            loss_evidence.append("benchmark_loss_absent")

        # SO_RXQ_OVFL is receive-side evidence. A sender's receive queue cannot
        # explain failure to emit the target load; only sender-side evidence can.
        if (sender_socket or 0) > 0:
            sender_evidence.append("sender_rxq_overflow_noncausal")
        if (receiver_socket or 0) > 0:
            loss_evidence.append("receiver_socket_overflow")
        if (sender_udp_send or 0) > 0:
            sender_evidence.append("sender_udp_sndbuf_errors")
            if benchmark_loss is None or benchmark_loss <= 0:
                sender_evidence.append("sender_host_udp_errors_uncorrelated")
        if (receiver_udp_buffer or 0) > 0:
            loss_evidence.append("receiver_udp_rcvbuf_errors")
        if (receiver_udp_errors or 0) > 0:
            loss_evidence.append("receiver_udp_in_errors")
        if ((receiver_udp_buffer or 0) > 0 or (receiver_udp_errors or 0) > 0):
            if benchmark_loss is None or benchmark_loss <= 0:
                loss_evidence.append("receiver_host_udp_errors_uncorrelated")

        sender_cpu, sender_cpu_strong = _endpoint_cpu_evidence(
            sender_endpoint_delta, "sender")
        receiver_cpu, receiver_cpu_strong = _endpoint_cpu_evidence(
            receiver_endpoint_delta, "receiver")
        sender_evidence.extend(sender_cpu)
        loss_evidence.extend(receiver_cpu)

        if target_attained:
            sender_classification = {
                "primary": "none", "scope": "none", "confidence": "high",
            }
        elif benchmark_loss and (sender_udp_send or 0) > 0:
            sender_classification = {
                "primary": "socket_overflow", "scope": "sender",
                "confidence": "medium",
            }
        elif sender_cpu:
            sender_classification = {
                "primary": "cpu", "scope": "sender",
                "confidence": "high" if sender_cpu_strong else "medium",
            }
        else:
            sender_evidence.append(
                "no_sender_pressure" if _endpoint_is_complete(sender_endpoint_delta)
                else "sender_telemetry_partial" if _endpoint_is_available(
                    sender_endpoint_delta) else "sender_telemetry_unavailable")
            sender_classification = {
                "primary": "unknown", "scope": "sender", "confidence": "low",
            }

        loss_observed = None if benchmark_loss is None else benchmark_loss > 0
        if loss_observed is False:
            loss_classification = {
                "primary": "none", "scope": "none", "confidence": "high",
            }
        elif (receiver_socket or 0) > 0:
            loss_classification = {
                "primary": "socket_overflow", "scope": "receiver",
                "confidence": "high",
            }
        elif loss_observed and (receiver_udp_buffer or 0) > 0:
            loss_classification = {
                "primary": "socket_overflow", "scope": "receiver",
                "confidence": "medium",
            }
        elif loss_observed and (receiver_udp_errors or 0) > 0:
            loss_classification = {
                "primary": "udp_receive_errors", "scope": "receiver",
                "confidence": "medium",
            }
        elif receiver_cpu:
            loss_classification = {
                "primary": "cpu", "scope": "receiver",
                "confidence": "high" if receiver_cpu_strong else "medium",
            }
        elif (_endpoint_is_complete(sender_endpoint_delta)
              and _endpoint_is_complete(receiver_endpoint_delta)):
            loss_evidence.append("no_endpoint_pressure")
            loss_classification = {
                "primary": "network_or_remote", "scope": "path",
                "confidence": "low",
            }
        else:
            sender_available = _endpoint_is_available(sender_endpoint_delta)
            receiver_available = _endpoint_is_available(receiver_endpoint_delta)
            loss_evidence.append(
                "endpoint_telemetry_partial" if sender_available or receiver_available
                else "endpoint_telemetry_unavailable")
            loss_classification = {
                "primary": "unknown", "scope": "unknown", "confidence": "low",
            }

        result.update(sender_classification)
        result["sender_attainment"].update(sender_classification)
        result["sender_attainment"]["evidence"] = sender_evidence
        result["loss_causality"].update(loss_classification)
        result["loss_causality"]["evidence"] = loss_evidence
        result["evidence"] = list(dict.fromkeys(sender_evidence + loss_evidence))
        return result
    except Exception:
        result.update({
            "primary": "unknown",
            "scope": "unknown",
            "confidence": "low",
            "evidence": ["classification_unavailable"],
        })
        result["sender_attainment"].update({
            "primary": "unknown", "scope": "unknown", "confidence": "low",
            "evidence": ["classification_unavailable"],
        })
        return result
