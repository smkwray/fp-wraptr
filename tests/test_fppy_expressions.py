import math

import numpy as np
import pandas as pd
import pytest

from fppy.expressions import evaluate_expression


def _isnan(value: float) -> bool:
    try:
        return bool(math.isnan(float(value)))
    except Exception:
        return False


def test_evaluate_expression_log_vectorized() -> None:
    df = pd.DataFrame({"X": [1.0, 2.0, 4.0]})
    out = evaluate_expression("LOG(X)", data=df)
    assert isinstance(out, pd.Series)
    assert out.tolist() == [0.0, math.log(2.0), math.log(4.0)]


def test_evaluate_expression_exp_vectorized() -> None:
    df = pd.DataFrame({"X": [0.0, 1.0, 2.0]})
    out = evaluate_expression("EXP(X)", data=df)
    assert out.tolist() == pytest.approx([1.0, math.e, math.e**2])


def test_evaluate_expression_abs_vectorized() -> None:
    df = pd.DataFrame({"X": [-2.0, 0.0, 3.5]})
    out = evaluate_expression("ABS(X)", data=df)
    assert out.tolist() == [2.0, 0.0, 3.5]


def test_evaluate_expression_log_with_lag() -> None:
    df = pd.DataFrame({"X": [1.0, 2.0, 4.0]})
    out = evaluate_expression("LOG(X(-1))", data=df)
    values = out.tolist()
    assert _isnan(values[0])
    assert values[1:] == [0.0, math.log(2.0)]


def test_evaluate_expression_preserves_series_index() -> None:
    idx = pd.Index(["a", "b", "c"], name="period")
    df = pd.DataFrame({"X": [1.0, 2.0, 4.0]}, index=idx)
    out = evaluate_expression("LOG(X)", data=df)
    assert out.index.equals(idx)


def test_evaluate_expression_supports_basic_arithmetic() -> None:
    df = pd.DataFrame({"X": [1.0, 2.0, 4.0]})
    out = evaluate_expression("X + 1", data=df)
    assert out.tolist() == [2.0, 3.0, 5.0]


def test_pd_eval_fp_surface_log_repro_works() -> None:
    fp_surface = type("FpSurface", (), {"log": staticmethod(np.log)})()
    out = pd.eval(
        "fp.log(L1Z)",
        engine="python",
        local_dict={"L1Z": np.array([1.0, 2.0, 4.0]), "fp": fp_surface},
    )
    assert list(out) == pytest.approx([0.0, math.log(2.0), math.log(4.0)])
