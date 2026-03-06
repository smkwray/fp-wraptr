"""Repo-wide runtime hygiene guardrails.

Python auto-imports `sitecustomize` (when present on `sys.path`) before
user-module imports. Setting `dont_write_bytecode` here prevents repo-local
`__pycache__` directories during common invocations like `python -m pytest`.
"""

from __future__ import annotations

import os
import sys

# Hard guard against repo-local `__pycache__`:
# - `dont_write_bytecode` prevents writes entirely for typical runs.
# - `pycache_prefix` ensures that if bytecode writes happen anyway (e.g. a tool
#   flips the flag), they go to an external temp location instead of the repo.
sys.dont_write_bytecode = True
try:
    sys.pycache_prefix = os.environ.get(
        "PYTHONPYCACHEPREFIX", "/tmp/fp-wraptr-pycache"
    )
except AttributeError:
    # Older Pythons may not expose `sys.pycache_prefix`.
    pass
