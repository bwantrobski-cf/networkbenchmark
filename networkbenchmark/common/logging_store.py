"""Datetime-stamped JSON + text result logging.

Every completed test (throughput, mtr, connectivity, pmtud, protocols)
writes a pair of files into the logs directory:
    YYYYMMDD-HHMMSS_<test_type>.json
    YYYYMMDD-HHMMSS_<test_type>.log
"""

import json
import os
from datetime import datetime

from . import constants


def _default_base_dir() -> str:
    """Project root that ships with the package (the directory containing the
    `networkbenchmark` package), so logs land in `<project>/logs/` regardless
    of the current working directory."""
    common_dir = os.path.dirname(os.path.abspath(__file__))       # .../networkbenchmark/common
    package_dir = os.path.dirname(common_dir)                     # .../networkbenchmark (package)
    return os.path.dirname(package_dir)                           # .../ (project root)


def get_log_dir(base_dir: str = None) -> str:
    base_dir = base_dir or _default_base_dir()
    log_dir = os.path.join(base_dir, constants.LOG_DIR_NAME)
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError:
        # Fall back to the current working directory (e.g. if the package is
        # installed into a read-only location such as site-packages).
        log_dir = os.path.join(os.getcwd(), constants.LOG_DIR_NAME)
        os.makedirs(log_dir, exist_ok=True)
    return log_dir


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def save_result(test_type: str, data: dict, text_lines, base_dir: str = None):
    """Write JSON + text log files for a completed test.

    Args:
        test_type: one of the TEST_TYPE_* constants (used in filename).
        data: JSON-serializable dict with full structured results.
        text_lines: iterable of strings for the human-readable log.
        base_dir: directory under which the `logs/` folder is created
            (defaults to current working directory).

    Returns:
        (json_path, text_path)
    """
    log_dir = get_log_dir(base_dir)
    ts = _timestamp()
    json_path = os.path.join(log_dir, f"{ts}_{test_type}.json")
    text_path = os.path.join(log_dir, f"{ts}_{test_type}.log")

    with open(json_path, "w") as f:
        json.dump(data, f, indent=2, default=str)

    with open(text_path, "w") as f:
        for line in text_lines:
            f.write(str(line) + "\n")

    return json_path, text_path


def list_logs(base_dir: str = None):
    log_dir = get_log_dir(base_dir)
    entries = sorted(os.listdir(log_dir), reverse=True)
    return [os.path.join(log_dir, e) for e in entries]
