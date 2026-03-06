"""Backward-compatible dashboard entrypoint shim.

Prefer launching ``Run_Manager.py`` so Streamlit shows the root page label as
``Run Manager`` in the left navigation.
"""

from __future__ import annotations

from apps.dashboard.Run_Manager import main


if __name__ == "__main__":
    main()
