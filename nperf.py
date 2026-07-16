#!/usr/bin/env python3
"""Directly-runnable launcher for NPERF.

Lets you start the program from a plain script with arguments, e.g.:

    python nperf.py webui
    python nperf.py server --bind 0.0.0.0
    python nperf.py tput --server-ip 10.0.0.5 --time 10
    sudo .venv/bin/python nperf.py webui     # for raw-socket diagnostics

This works (unlike running a module inside the package directly) because the
launcher sits beside the `nperf` package and uses an absolute
import instead of a package-relative one.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nperf.cli import main

if __name__ == "__main__":
    main()
