"""Attach-rule helpers for wiring generated include files into input scripts."""

from __future__ import annotations

from fp_wraptr.scenarios.authoring.models import AttachRule


def render_input_include_line(include_file: str) -> str:
    return f"INPUT FILE={str(include_file).strip()};"


def apply_attach_rule(input_text: str, *, rule: AttachRule, include_file: str) -> str:
    """Apply one attach rule to an input script body."""
    line = render_input_include_line(include_file)
    text = str(input_text)
    lines = text.splitlines()

    if rule.kind == "overlay_file":
        return text

    if rule.kind == "replace_include":
        for idx, current in enumerate(lines):
            if rule.match_text and rule.match_text in current:
                lines[idx] = line
                return _join_lines(lines, source=text)
        raise ValueError(f"replace_include match_text not found: {rule.match_text!r}")

    if rule.kind == "append_include_after_match":
        for idx, current in enumerate(lines):
            if rule.match_text and rule.match_text in current:
                lines.insert(idx + 1, line)
                return _join_lines(lines, source=text)
        raise ValueError(f"append_include_after_match match_text not found: {rule.match_text!r}")

    if rule.kind == "append_include_before_return":
        for idx, current in enumerate(lines):
            if current.strip().upper().startswith("RETURN"):
                lines.insert(idx, line)
                return _join_lines(lines, source=text)
        lines.append(line)
        return _join_lines(lines, source=text)

    raise ValueError(f"Unsupported attach rule: {rule.kind!r}")


def _join_lines(lines: list[str], *, source: str) -> str:
    if source.endswith("\n"):
        return "\n".join(lines) + "\n"
    return "\n".join(lines)

