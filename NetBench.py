#!/usr/bin/env python3
"""Directly-runnable launcher for NetworkBenchmark.

Lets you start the program from a plain script with arguments, e.g.:

    python NetBench.py webui
    python NetBench.py server --bind 0.0.0.0
    python NetBench.py throughput --server-ip 10.0.0.5 --time 10
    sudo .venv/bin/python NetBench.py webui     # for raw-socket diagnostics

This works (unlike running a module inside the package directly) because the
launcher sits beside the `networkbenchmark` package and uses an absolute
import instead of a package-relative one.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from networkbenchmark.cli import main

if __name__ == "__main__":
    main()
