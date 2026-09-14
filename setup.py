import platform
import sys

from setuptools import Extension, setup


ext_modules = []
if sys.platform.startswith("linux") and platform.python_implementation() == "CPython":
    ext_modules.append(
        Extension(
            "nperf.transport._udp_batch",
            sources=["nperf/transport/_udp_batch.c"],
            optional=True,
        )
    )


setup(ext_modules=ext_modules)
