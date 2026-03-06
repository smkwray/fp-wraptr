from __future__ import annotations

import pandas as pd

from fppy.mini_run import _infer_printvar_variable_order


def test_infer_printvar_order_appends_non_fmout_columns(tmp_path) -> None:
    # The variable-map heuristic in `_infer_printvar_variable_order` should not
    # drop variables that exist in the runtime frame but do not appear in fmout.
    (tmp_path / "fmout_coefs.txt").write_text(
        "\n".join(
            [
                "A 1 B 2",
                "",
            ]
        ),
        encoding="utf-8",
    )
    frame = pd.DataFrame(
        {
            "B": [2.0],
            "A": [1.0],
            "EXTRA": [3.0],
        },
        index=pd.Index(["2025.4"], name="smpl"),
    )

    order = _infer_printvar_variable_order(frame, runtime_base_dir=tmp_path)

    assert order[:2] == ("A", "B")
    assert "EXTRA" in order

