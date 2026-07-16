"""Shared latency statistics helpers (used by throughput RTT probing and MTR)."""

import statistics


def compute_latency_stats(samples):
    """Given a list of latency samples (ms), return avg/best/worst/stddev/jitter.

    jitter is the mean absolute difference between consecutive samples
    (RFC 3550-style variation), distinct from stddev (population spread).
    """
    samples = [s for s in samples if s is not None]
    if not samples:
        return {
            "count": 0,
            "avg": None,
            "best": None,
            "worst": None,
            "stddev": None,
            "jitter": None,
        }

    count = len(samples)
    avg = sum(samples) / count
    best = min(samples)
    worst = max(samples)
    stddev = statistics.pstdev(samples) if count > 1 else 0.0

    if count > 1:
        diffs = [abs(samples[i] - samples[i - 1]) for i in range(1, count)]
        jitter = sum(diffs) / len(diffs)
    else:
        jitter = 0.0

    return {
        "count": count,
        "avg": avg,
        "best": best,
        "worst": worst,
        "stddev": stddev,
        "jitter": jitter,
    }
