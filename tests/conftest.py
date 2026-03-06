"""Global pytest test-session hygiene guardrails."""

from __future__ import annotations

import sys

# Prevent test imports from writing repo-local __pycache__ entries.
sys.dont_write_bytecode = True
