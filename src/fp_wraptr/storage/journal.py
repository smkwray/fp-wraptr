"""Run journal — SQLite-backed log of all fp-wraptr scenario runs.

Stores a record for every run with timestamp, config hash, key output
variables, and metadata. Supports querying by variable thresholds and
listing recent runs.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fp_wraptr.scenarios.runner import ScenarioResult

__all__ = ["JournalEntry", "RunJournal"]

_DEFAULT_DB_PATH = Path.home() / ".fp-wraptr" / "runs.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    scenario_name TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    output_dir TEXT,
    success INTEGER NOT NULL DEFAULT 0,
    forecast_start TEXT,
    forecast_end TEXT,
    n_variables INTEGER DEFAULT 0,
    key_outputs TEXT,
    config_json TEXT,
    metadata TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_timestamp ON runs(timestamp);
CREATE INDEX IF NOT EXISTS idx_runs_scenario ON runs(scenario_name);
"""


@dataclass
class JournalEntry:
    """A single run journal record."""

    id: int | None = None
    timestamp: str = ""
    scenario_name: str = ""
    config_hash: str = ""
    output_dir: str = ""
    success: bool = False
    forecast_start: str = ""
    forecast_end: str = ""
    n_variables: int = 0
    key_outputs: dict[str, float] = field(default_factory=dict)
    config_json: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)


class RunJournal:
    """SQLite-backed run journal.

    Args:
        db_path: Path to the SQLite database file.
            Defaults to ``~/.fp-wraptr/runs.db``.
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = Path(db_path) if db_path else _DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._ensure_schema()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self._get_conn()
        conn.executescript(_SCHEMA)
        conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def log_run(self, result: ScenarioResult) -> JournalEntry:
        """Log a completed scenario run to the journal.

        Args:
            result: The completed ScenarioResult.

        Returns:
            The created JournalEntry with assigned id.
        """
        config = result.config
        config_data = config.model_dump(mode="json")
        config_hash = hashlib.sha256(json.dumps(config_data, sort_keys=True).encode()).hexdigest()[
            :16
        ]

        key_outputs: dict[str, float] = {}
        n_variables = 0
        forecast_start = ""
        forecast_end = ""

        if result.parsed_output:
            forecast_start = result.parsed_output.forecast_start
            forecast_end = result.parsed_output.forecast_end
            n_variables = len(result.parsed_output.variables)

            # Store final-period level for tracked variables
            track = config.track_variables or list(result.parsed_output.variables.keys())[:20]
            for var_name in track:
                if var_name in result.parsed_output.variables:
                    levels = result.parsed_output.variables[var_name].levels
                    if levels:
                        key_outputs[var_name] = levels[-1]

        entry = JournalEntry(
            timestamp=result.timestamp,
            scenario_name=config.name,
            config_hash=config_hash,
            output_dir=str(result.output_dir),
            success=result.success,
            forecast_start=forecast_start,
            forecast_end=forecast_end,
            n_variables=n_variables,
            key_outputs=key_outputs,
            config_json=config_data,
        )

        conn = self._get_conn()
        cursor = conn.execute(
            """INSERT INTO runs
               (timestamp, scenario_name, config_hash, output_dir, success,
                forecast_start, forecast_end, n_variables, key_outputs,
                config_json, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                entry.timestamp,
                entry.scenario_name,
                entry.config_hash,
                entry.output_dir,
                1 if entry.success else 0,
                entry.forecast_start,
                entry.forecast_end,
                entry.n_variables,
                json.dumps(entry.key_outputs),
                json.dumps(entry.config_json),
                json.dumps(entry.metadata),
            ),
        )
        conn.commit()
        entry.id = cursor.lastrowid
        return entry

    def list_runs(self, last: int = 20) -> list[JournalEntry]:
        """List the most recent journal entries.

        Args:
            last: Maximum number of entries to return.

        Returns:
            List of JournalEntry, most recent first.
        """
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (last,)).fetchall()
        return [self._row_to_entry(row) for row in rows]

    def search_runs(self, where: str) -> list[JournalEntry]:
        """Search runs by variable threshold expressions.

        Supports simple expressions like "UR > 5.0", "GDPR < 100",
        "UR > 5.0 AND PCY < 0".

        Args:
            where: Filter expression using variable names and comparisons.

        Returns:
            List of matching JournalEntry records.
        """
        conn = self._get_conn()
        all_rows = conn.execute("SELECT * FROM runs ORDER BY id DESC").fetchall()

        conditions = _parse_conditions(where)
        results = []
        for row in all_rows:
            entry = self._row_to_entry(row)
            if _matches_conditions(entry.key_outputs, conditions):
                results.append(entry)

        return results

    def get_run(self, run_id: int) -> JournalEntry | None:
        """Get a specific journal entry by ID."""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_entry(row)

    def delete_run(self, run_id: int) -> bool:
        """Delete a journal entry by ID."""
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
        conn.commit()
        return cursor.rowcount > 0

    @staticmethod
    def _row_to_entry(row: sqlite3.Row) -> JournalEntry:
        return JournalEntry(
            id=row["id"],
            timestamp=row["timestamp"],
            scenario_name=row["scenario_name"],
            config_hash=row["config_hash"],
            output_dir=row["output_dir"] or "",
            success=bool(row["success"]),
            forecast_start=row["forecast_start"] or "",
            forecast_end=row["forecast_end"] or "",
            n_variables=row["n_variables"] or 0,
            key_outputs=json.loads(row["key_outputs"] or "{}"),
            config_json=json.loads(row["config_json"] or "{}"),
            metadata=json.loads(row["metadata"] or "{}"),
        )


@dataclass
class _Condition:
    variable: str
    operator: str
    value: float


def _parse_conditions(where: str) -> list[_Condition]:
    """Parse a simple filter expression into conditions.

    Supports: "UR > 5.0", "GDPR < 100", "UR > 5 AND PCY < 0"
    """
    import re

    conditions: list[_Condition] = []
    # Split on AND (case-insensitive)
    parts = re.split(r"\s+AND\s+", where, flags=re.IGNORECASE)
    pattern = re.compile(r"(\w+)\s*(>=|<=|>|<|==|!=)\s*([-\d.]+)")

    for part in parts:
        match = pattern.search(part.strip())
        if match:
            conditions.append(
                _Condition(
                    variable=match.group(1),
                    operator=match.group(2),
                    value=float(match.group(3)),
                )
            )

    return conditions


def _matches_conditions(
    key_outputs: dict[str, float],
    conditions: list[_Condition],
) -> bool:
    """Check if key_outputs satisfy all conditions."""
    for cond in conditions:
        if cond.variable not in key_outputs:
            return False
        actual = key_outputs[cond.variable]
        if cond.operator == ">" and not (actual > cond.value):
            return False
        if cond.operator == ">=" and not (actual >= cond.value):
            return False
        if cond.operator == "<" and not (actual < cond.value):
            return False
        if cond.operator == "<=" and not (actual <= cond.value):
            return False
        if cond.operator == "==" and actual != cond.value:
            return False
        if cond.operator == "!=" and actual == cond.value:
            return False
    return True
