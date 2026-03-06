"""Dependency extraction and deterministic ordering for definition commands."""

from __future__ import annotations

import bisect
import re
from collections.abc import Iterable
from dataclasses import dataclass

from fppy.expressions import parse_assignment
from fppy.parser import FPCommand, FPCommandRecord

_IDENT_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
_EQ_INLINE_RE = re.compile(
    r"^\s*EQ\s+\d+\s+(?P<lhs>[A-Za-z_][A-Za-z0-9_]*)\s+(?P<rhs>.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_FUNC_TOKENS = {
    "ABS",
    "COEF",
    "EXP",
    "LOG",
    "MAX",
    "MIN",
    "POW",
    "SQRT",
}
_ASSIGNMENT_COMMANDS = {
    FPCommand.GENR,
    FPCommand.IDENT,
    FPCommand.LHS,
    FPCommand.CREATE,
}


def _extract_rhs_variables(rhs: str) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for match in _IDENT_RE.finditer(rhs):
        token = match.group(0).upper()
        if token in _FUNC_TOKENS:
            continue
        if token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return tuple(ordered)


@dataclass(frozen=True)
class DefinitionStep:
    line_number: int
    command: FPCommand
    statement: str
    lhs: str
    rhs: str = ""
    rhs_variables: tuple[str, ...] = ()
    index: int = 0

    def __post_init__(self) -> None:
        lhs = str(self.lhs).strip().upper()
        object.__setattr__(self, "lhs", lhs)

        rhs = str(self.rhs).strip()
        object.__setattr__(self, "rhs", rhs)

        if self.rhs_variables:
            normalized = tuple(str(token).upper() for token in self.rhs_variables)
        elif rhs:
            normalized = tuple(token for token in _extract_rhs_variables(rhs) if token != lhs)
        else:
            normalized = ()
        object.__setattr__(self, "rhs_variables", normalized)


@dataclass(frozen=True)
class DependencyOrderResult:
    steps: tuple[DefinitionStep, ...]
    order: tuple[DefinitionStep, ...]
    unresolved_references: dict[int, set[str]]
    edge_count: int
    cyclic: bool


def _parse_eq_inline_statement(statement: str) -> tuple[str, str, tuple[str, ...]] | None:
    match = _EQ_INLINE_RE.match(statement.strip())
    if match is None:
        return None
    lhs = match.group("lhs").strip().upper()
    rhs = match.group("rhs").strip()
    if not lhs:
        return None
    return lhs, rhs, _extract_rhs_variables(rhs)


def extract_definition_steps(
    records: Iterable[FPCommandRecord],
    *,
    include_eq: bool = True,
) -> list[DefinitionStep]:
    steps: list[DefinitionStep] = []
    for record in records:
        if record.command in _ASSIGNMENT_COMMANDS:
            try:
                parsed = parse_assignment(record.statement)
            except ValueError:
                continue
            lhs = parsed.lhs.strip().upper()
            if not lhs:
                continue
            rhs_variables = tuple(
                token for token in _extract_rhs_variables(parsed.rhs) if token != lhs
            )
            steps.append(
                DefinitionStep(
                    line_number=record.line_number,
                    command=record.command,
                    statement=record.statement,
                    lhs=lhs,
                    rhs=parsed.rhs,
                    rhs_variables=rhs_variables,
                    index=len(steps),
                )
            )
            continue

        if include_eq and record.command == FPCommand.EQ:
            parsed_eq = _parse_eq_inline_statement(record.statement)
            if parsed_eq is None:
                continue
            lhs, rhs, rhs_variables = parsed_eq
            rhs_filtered = tuple(token for token in rhs_variables if token != lhs)
            steps.append(
                DefinitionStep(
                    line_number=record.line_number,
                    command=record.command,
                    statement=record.statement,
                    lhs=lhs,
                    rhs=rhs,
                    rhs_variables=rhs_filtered,
                    index=len(steps),
                )
            )
    return steps


def extract_dependencies(steps: Iterable[DefinitionStep]) -> dict[str, set[str]]:
    normalized = list(steps)
    defined = {step.lhs for step in normalized}
    dependencies: dict[str, set[str]] = {}
    for step in normalized:
        rhs_tokens = step.rhs_variables or _extract_rhs_variables(step.rhs)
        dependencies[step.lhs] = {
            token for token in rhs_tokens if token in defined and token != step.lhs
        }
    return dependencies


def _build_edges(
    steps: list[DefinitionStep],
) -> tuple[set[tuple[int, int]], dict[int, set[str]]]:
    indices_by_lhs: dict[str, list[int]] = {}
    for index, step in enumerate(steps):
        indices_by_lhs.setdefault(step.lhs, []).append(index)

    edges: set[tuple[int, int]] = set()
    unresolved: dict[int, set[str]] = {}

    for target_index, step in enumerate(steps):
        step_unresolved: set[str] = set()
        for dependency in step.rhs_variables:
            sources = indices_by_lhs.get(dependency)
            if not sources:
                step_unresolved.add(dependency)
                continue
            source_index = sources[-1]
            if source_index == target_index:
                continue
            edges.add((source_index, target_index))
        if step_unresolved:
            unresolved[step.line_number] = step_unresolved

    return edges, unresolved


def order_definition_steps(steps: Iterable[DefinitionStep]) -> list[DefinitionStep]:
    ordered_input = list(steps)
    if len(ordered_input) <= 1:
        return ordered_input

    edges, _unresolved = _build_edges(ordered_input)
    node_count = len(ordered_input)
    adjacency: list[set[int]] = [set() for _ in range(node_count)]
    indegree = [0] * node_count
    for source, target in edges:
        if target not in adjacency[source]:
            adjacency[source].add(target)
            indegree[target] += 1

    available = [idx for idx in range(node_count) if indegree[idx] == 0]
    output_indices: list[int] = []
    visited = [False] * node_count

    while available:
        current = available.pop(0)
        if visited[current]:
            continue
        visited[current] = True
        output_indices.append(current)
        for neighbor in sorted(adjacency[current]):
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                bisect.insort(available, neighbor)

    for idx in range(node_count):
        if not visited[idx]:
            output_indices.append(idx)

    return [ordered_input[idx] for idx in output_indices]


def build_dependency_order(
    records: Iterable[FPCommandRecord],
    *,
    include_eq: bool = True,
) -> DependencyOrderResult:
    steps = extract_definition_steps(records, include_eq=include_eq)
    ordered = order_definition_steps(steps)
    edges, unresolved = _build_edges(steps)
    cyclic = tuple(step.line_number for step in ordered) != tuple(
        step.line_number for step in steps
    ) and bool(edges)
    if not cyclic:
        # Detect pure cycle cases where output ordering fell back to input.
        node_count = len(steps)
        if node_count > 0:
            indegree = [0] * node_count
            adjacency: list[set[int]] = [set() for _ in range(node_count)]
            for source, target in edges:
                if target not in adjacency[source]:
                    adjacency[source].add(target)
                    indegree[target] += 1
            queue = [idx for idx, degree in enumerate(indegree) if degree == 0]
            visited = 0
            while queue:
                current = queue.pop(0)
                visited += 1
                for neighbor in adjacency[current]:
                    indegree[neighbor] -= 1
                    if indegree[neighbor] == 0:
                        bisect.insort(queue, neighbor)
            cyclic = visited < node_count

    return DependencyOrderResult(
        steps=tuple(steps),
        order=tuple(ordered),
        unresolved_references=unresolved,
        edge_count=len(edges),
        cyclic=cyclic,
    )


__all__ = [
    "DefinitionStep",
    "DependencyOrderResult",
    "build_dependency_order",
    "extract_definition_steps",
    "extract_dependencies",
    "order_definition_steps",
]
