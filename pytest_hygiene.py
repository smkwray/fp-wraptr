"""Pytest startup hygiene hook.

Loaded via `-p pytest_hygiene` from pytest addopts so it runs before test
module imports and disables bytecode writes in-repo.
"""

from __future__ import annotations

import sys

sys.dont_write_bytecode = True
