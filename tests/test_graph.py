"""Tests for FP input dependency graphs."""

from __future__ import annotations

import pytest

pytest.importorskip("networkx")

from fp_wraptr.analysis.graph import (
    build_dependency_graph,
    get_downstream,
    get_upstream,
    summarize_graph,
)


def test_build_graph_empty_input():
    graph = build_dependency_graph({})

    assert graph.number_of_nodes() == 0
    assert graph.number_of_edges() == 0
    assert summarize_graph(graph) == {
        "nodes": 0,
        "edges": 0,
        "roots": [],
        "leaves": [],
        "most_connected": [],
    }


def test_build_graph_with_cycle():
    parsed_input = {
        "equations": [
            {"lhs": "AA", "rhs": "BB CC"},
            {"lhs": "BB", "rhs": "AA"},
        ],
        "identities": [],
        "generated_vars": [],
    }

    graph = build_dependency_graph(parsed_input)
    assert graph.has_edge("BB", "AA")
    assert graph.has_edge("CC", "AA")
    assert graph.has_edge("AA", "BB")
    assert get_upstream(graph, "AA") == {"BB", "CC"}
    assert get_downstream(graph, "AA") == {"BB"}


def test_build_graph_multi_level_dependencies():
    parsed_input = {
        "equations": [
            {"lhs": "AA", "rhs": "BB"},
            {"lhs": "BB", "rhs": "CC"},
            {"lhs": "CC", "rhs": "DD"},
        ],
        "identities": [{"name": "DD", "expression": "EE + 1"}],
        "generated_vars": [{"name": "EE", "expression": "FF + 2"}],
    }

    graph = build_dependency_graph(parsed_input)
    assert summarize_graph(graph)["nodes"] == 6
    assert summarize_graph(graph)["edges"] == 5
    assert get_upstream(graph, "AA") == {"BB", "CC", "DD", "EE", "FF"}
    assert get_downstream(graph, "FF") == {"AA", "BB", "CC", "DD", "EE"}


def test_summarize_graph_stats():
    parsed_input = {
        "equations": [
            {"lhs": "AA", "rhs": "BB CC"},
            {"lhs": "BB", "rhs": "CC"},
            {"lhs": "CC", "rhs": "DD"},
        ],
        "identities": [],
        "generated_vars": [],
    }
    graph = build_dependency_graph(parsed_input)
    summary = summarize_graph(graph)

    assert set(summary.keys()) == {"nodes", "edges", "roots", "leaves", "most_connected"}
    assert summary["nodes"] == 4
    assert summary["edges"] == 4


def test_get_upstream_nonexistent_variable():
    parsed_input = {
        "equations": [
            {"lhs": "AA", "rhs": "BB CC"},
            {"lhs": "BB", "rhs": "AA"},
        ],
        "identities": [],
        "generated_vars": [],
    }
    graph = build_dependency_graph(parsed_input)
    assert get_upstream(graph, "MISSING") == set()
