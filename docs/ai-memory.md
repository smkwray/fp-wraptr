# AI Memory Onboarding

This repository uses a dual-lane AI workflow for safe, parallel work across assistants.

## Canonical layout

- `do/orca.md` — orchestrator prompt and delegation policy.
- `do/todo.md`, `do/changes.md`, `do/dontdo.md`, `do/handoff.md` — Orca-owned coordination docs.
- `do/cani/` — Cani lane files (`subsys.md`, `woof.md`, `todo.md`, `dontdo.md`, `changes.md`, `inbox.md`).
- `do/feli/` — Feli lane files (`subsys.md`, `purr.md`, `todo.md`, `dontdo.md`, `changes.md`, `inbox.md`).
- `do/tandy.md` and `do/spark*.md` — batch task routing helpers.

Source of truth for lane protocol details is `do/memory-system.md` in the repository root.

## Required read order

- **Orchestrator (Orca) turn**: `orca.md` → `handoff.md` → `dontdo.md` → latest `changes.md` → `todo.md`.
- **Lane (Cani/Feli) turn**: `<lane>/subsys.md` → lane handoff (`woof.md`/`purr.md`) → `<lane>/dontdo.md` → latest `<lane>/changes.md` → `<lane>/todo.md` → `<lane>/inbox.md`.

## Update rules

- **Orca updates**: write only coordinator files (`handoff.md`, `todo.md`, `changes.md`, `dontdo.md`) and end turns with 2–6 next steps.
- **Lane updates**: update only lane-local files listed above and keep `inbox.md` append-only history.
- Use explicit task IDs in `CANI-####` / `FELI-####` format when delegating or tracking work.

## Inbox protocol

- Orchestrator appends tasks to the target lane `inbox.md` with complete context and acceptance criteria.
- Lane claims/imports tasks into its own `todo.md`, executes, then marks the inbox line `claimed/imported` with date and result.
- Never delete inbox history.

## Guardrails

- **No lane collisions**: only one agent edits a given in-scope file unless explicitly delegated.
- **Orca is the only writer of global docs** (`do/handoff.md`, `do/todo.md`, `do/dontdo.md`, `do/changes.md`).
- **Lane inboxes are append-only**; lane edits never remove historical entries or bypass `inbox.md`.
