"""Browse model equations, identities, and generated variables."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import page_favicon
from fp_wraptr.io.input_parser import parse_fp_input, parse_fp_input_text


def _load_parsed(uploaded, path: str) -> dict | None:
    if uploaded is not None:
        text = uploaded.getvalue().decode("utf-8", errors="replace")
        return parse_fp_input_text(text)
    if path:
        p = Path(path)
        if p.exists():
            return parse_fp_input(p)
        st.error(f"Path not found: {p}")
    return None


def _format_expression(expr: str) -> str:
    """Light cleanup of FP DSL expressions for display."""
    expr = expr.strip()
    # Remove trailing EQUATION labels (e.g. "EQUATION 1")
    import re

    expr = re.sub(r"\bEQUATION\s+\d+\s*$", "", expr, flags=re.IGNORECASE).strip()
    return expr


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Equations", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title(
        "Model Equations",
        caption="Browse behavioral equations, identities, and generated variables in an FP input file.",
    )

    uploaded = st.file_uploader("Upload fminput.txt", type=["txt"])
    path = st.text_input("Or load from path", value="FM/fminput.txt")

    parsed = _load_parsed(uploaded, path)
    if parsed is None:
        st.info("Upload an fminput.txt file or provide a valid path to browse equations.")
        return

    equations = parsed.get("equations", [])
    identities = parsed.get("identities", [])
    generated = parsed.get("generated_vars", [])
    equation_lhs = parsed.get("equation_lhs", [])

    # Build a lookup of LHS expressions keyed by position
    lhs_lookup: dict[int, str] = {}
    for i, lhs in enumerate(equation_lhs):
        lhs_lookup[i] = lhs.get("expression", "")

    st.sidebar.header("Filter")
    section = st.sidebar.radio(
        "Section",
        ["All", "Behavioral Equations", "Identities", "Generated Variables"],
    )
    search = st.sidebar.text_input("Search variable name").strip().upper()

    # Summary metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Behavioral Equations", len(equations))
    c2.metric("Identities", len(identities))
    c3.metric("Generated Variables", len(generated))

    # Behavioral Equations
    if section in ("All", "Behavioral Equations"):
        st.subheader("Behavioral Equations")
        st.markdown(
            "Estimated via 2SLS. Each equation has a dependent variable (LHS) and explanatory variables (RHS)."
        )

        if not equations:
            st.info("No behavioral equations found in this input file.")
        else:
            for eq in equations:
                eq_num = eq.get("number", "?")
                lhs_var = eq.get("lhs", "?")
                rhs = _format_expression(eq.get("rhs", ""))

                if search and search not in lhs_var.upper() and search not in rhs.upper():
                    continue

                with st.expander(f"EQ {eq_num}: {lhs_var}", expanded=False):
                    st.markdown(f"**Dependent variable:** `{lhs_var}`")
                    if rhs:
                        st.markdown("**RHS specification:**")
                        st.code(rhs, language="text")
                    opts = eq.get("options", {})
                    if opts:
                        st.markdown(f"**Options:** {opts}")

    # Identities
    if section in ("All", "Identities"):
        st.subheader("Identities")
        st.markdown("Accounting identities — hold by definition, not estimated.")

        if not identities:
            st.info("No identities found in this input file.")
        else:
            for ident in identities:
                name = ident.get("name", "?")
                expr = ident.get("expression", "")

                if search and search not in name.upper() and search not in expr.upper():
                    continue

                with st.expander(f"IDENT: {name}", expanded=False):
                    st.markdown(f"**Variable:** `{name}`")
                    st.markdown("**Definition:**")
                    st.code(f"{name} = {expr}", language="text")

    # Generated Variables
    if section in ("All", "Generated Variables"):
        st.subheader("Generated Variables")
        st.markdown("Computed transformations (GENR commands) — logs, ratios, lags, etc.")

        if not generated:
            st.info("No generated variables found in this input file.")
        else:
            for gen in generated:
                name = gen.get("name", "?")
                expr = gen.get("expression", "")

                if search and search not in name.upper() and search not in expr.upper():
                    continue

                with st.expander(f"GENR: {name}", expanded=False):
                    st.markdown(f"**Variable:** `{name}`")
                    st.markdown("**Expression:**")
                    st.code(f"{name} = {expr}", language="text")


if __name__ == "__main__":
    main()
