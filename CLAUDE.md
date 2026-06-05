# CLAUDE.md

## What we're building

A masterless, LAN file-synchronization system in Elixir/OTP — a learning project
(Syncthing-like). Each node keeps a local copy of a shared folder; changes
replicate peer-to-peer with **version vectors** for conflict detection,
**tombstones** for deletes, and **keep-both `sync-conflict` copies** for
concurrent edits. App name: `sync_set`.

The goal is to brush up on **distributed-systems theory** and **Elixir/OTP**.

## How I (Claude) should work on this project

**Do not write implementation or test code.** The human writes all code. Instead:
- Explain *what* to build and *why*.
- Name the Elixir/OTP concepts and specific APIs to use.
- Give struct **shapes** (field sketches), **pseudocode**, and the test **cases** to cover.
- Only write actual Elixir code when the human **explicitly asks** for it in-session
  (e.g. "show me the code for X"). Then keep it to the snippet requested.

This applies to the plan docs (already code-free) and to live sessions.

## Where things live

- **Design spec:** `docs/superpowers/specs/2026-06-04-replicated-folder-sync-design.md`
- **Plan 1 (correctness core — version vectors, conflict resolution, replica,
  convergence test, persisted Index):** `docs/superpowers/plans/2026-06-05-sync-core-plan-1.md`
- **Plan 2 (distribution & IO — watcher, control plane, TCP data plane,
  reconciler, libcluster, integration tests):** `docs/superpowers/plans/2026-06-05-sync-distribution-plan-2.md`

Plans are guided exercises with checkbox steps. When the human says e.g. "I'm on
Task 5 and need help with X", read the relevant task in the plan for context
before answering.

## Conventions

- TDD: failing test → implement → green → commit, one small commit per task step.
- End commit messages with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
