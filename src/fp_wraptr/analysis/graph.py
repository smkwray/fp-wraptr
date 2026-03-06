"""Build and analyze FP dependency graphs."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import networkx as nx


_VAR_REF_RE = re.compile(r"\b([A-Z][A-Z0-9_]{1,})\b")


def _as_upper(token: object) -> str:
    if not isinstance(token, str):
        return ""
    token = token.strip()
    return token.upper() if token else ""


def _extract_refs(expression: str) -> list[str]:
    return [_as_upper(match.group(1)) for match in _VAR_REF_RE.finditer(expression)]


def _load_networkx():
    try:
        import networkx as nx

    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "networkx is required for graph support. "
            "Install with `uv pip install fp-wraptr[graph]`."
        ) from exc
    return nx


def _iter_dependency_records(parsed_input: dict) -> Iterable[tuple[str, str]]:
    for equation in parsed_input.get("equations", []):
        lhs = _as_upper(equation.get("lhs"))
        rhs = equation.get("rhs", "")
        if lhs:
            for ref in _extract_refs(rhs):
                if ref:
                    yield lhs, ref

    for identity in parsed_input.get("identities", []):
        lhs = _as_upper(identity.get("name"))
        expression = identity.get("expression", "")
        if lhs:
            for ref in _extract_refs(expression):
                if ref:
                    yield lhs, ref

    for generator in parsed_input.get("generated_vars", []):
        lhs = _as_upper(generator.get("name"))
        expression = generator.get("expression", "")
        if lhs:
            for ref in _extract_refs(expression):
                if ref:
                    yield lhs, ref


def build_dependency_graph(parsed_input: dict) -> nx.DiGraph:
    """Build a variable dependency graph.

    The graph has a directed edge ``B -> A`` when variable ``A`` depends on ``B``
    in any supported command type.
    """
    nx = _load_networkx()
    graph = nx.DiGraph()

    for lhs, rhs in _iter_dependency_records(parsed_input):
        graph.add_node(lhs)
        graph.add_node(rhs)
        graph.add_edge(rhs, lhs)

    return graph


def get_upstream(graph: nx.DiGraph, variable: str) -> set[str]:
    """Return all ancestors of ``variable``."""
    nx = _load_networkx()
    if variable not in graph:
        return set()
    return set(nx.ancestors(graph, variable))


def get_downstream(graph: nx.DiGraph, variable: str) -> set[str]:
    """Return all descendants of ``variable``."""
    nx = _load_networkx()
    if variable not in graph:
        return set()
    return set(nx.descendants(graph, variable))


def summarize_graph(graph: nx.DiGraph) -> dict:
    """Summarize node/edge counts and core structural diagnostics."""
    if graph is None:
        return {
            "nodes": 0,
            "edges": 0,
            "roots": [],
            "leaves": [],
            "most_connected": [],
        }

    roots = sorted([node for node, degree in graph.in_degree() if degree == 0])
    leaves = sorted([node for node, degree in graph.out_degree() if degree == 0])
    if graph.number_of_nodes() == 0:
        most_connected: list[str] = []
    else:
        max_degree = max((graph.degree(node) for node in graph.nodes), default=0)
        most_connected = sorted([node for node in graph.nodes if graph.degree(node) == max_degree])

    return {
        "nodes": graph.number_of_nodes(),
        "edges": graph.number_of_edges(),
        "roots": roots,
        "leaves": leaves,
        "most_connected": most_connected,
    }
