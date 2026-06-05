# Sync Distribution & IO (Plan 2 of 2) Implementation Plan

> **For agentic workers:** This plan is a guided exercise. It describes WHAT to build, which Elixir/OTP concepts to use, the test cases to cover, and pseudocode — but **the human writes the code.** Do not write implementation or test code unless explicitly asked. See `CLAUDE.md` at the repo root. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Turn the Plan 1 correctness core into a running, multi-node sync node: watch a local directory, detect stable changes, announce them over Erlang distribution, transfer file bytes over TCP, reconcile via anti-entropy on (re)connect, and wire it all under a supervision tree discovered with libcluster.

**Architecture:** Every node runs an identical supervision tree. The **control plane** (announces, wants, index exchange) rides Erlang distribution as small messages between named processes (`{name, node}`); the **data plane** ships file bytes over a separate `:gen_tcp` socket. The Plan 1 `Index` GenServer remains the single source of truth; networking components read/update it. The echo-loop is killed structurally: replicated writes update the `Index` before touching the file, and `LocalChange` ignores any watcher event whose checksum already matches the `Index`.

**Tech Stack:** Elixir 1.20 / OTP 29; `file_system` (FS watching); `:gen_tcp` (data plane, built-in); Erlang distribution + `:net_kernel.monitor_nodes` (control plane, built-in); `libcluster` (discovery); `local_cluster` or `:peer` (multi-node tests).

**Depends on Plan 1 APIs:** `SyncSet.Index` (`get/2`, `snapshot/1`, `local_write/4`, `local_delete/2`, `apply_remote/3`), `SyncSet.Entry`, `SyncSet.Checksum`, `SyncSet.Replica`. **Placeholder note:** exact arities/return shapes below assume Plan 1 as specified — confirm against the code as actually built and adjust call sites. Anywhere marked `[CONFIRM vs Plan 1]` is a known dependency point.

**Spec:** `docs/superpowers/specs/2026-06-04-replicated-folder-sync-design.md`

**TDD rhythm:** failing test → fail for the right reason → implement → green → commit. Prefer testing each component through a thin, fakeable seam (inject a test pid/module) before the real integration tasks. Commit trailer per `CLAUDE.md`.

---

### Task 1: Runtime configuration

**Files:** `lib/sync_set/config.ex` (or use `Application` env directly), `config/config.exs`, `config/runtime.exs`.

**What you're building:** a small accessor for per-node settings: `sync_dir` (the replicated folder), `dets_path`, `data_port` (TCP), `node_id`, and the libcluster topology.

**Concepts/APIs:** `Application.get_env/3`, `config/runtime.exs` for values that must be read at boot (ports, paths from env vars), `System.get_env/2`. Keep `node_id` derived from `Node.self()` so it stays consistent with version-vector keys. **[CONFIRM vs Plan 1]** the atom you use as `node_id` must match what `Index`/`Replica` use as the version-vector key (`Node.self()`).

**Concept to brush up:** Mix config vs runtime config, and why ports/paths belong in `runtime.exs`.

- [ ] **Step 1:** Decide the config keys and defaults; write a tiny test that the accessor returns defaults and respects overrides (set `Application.put_env` in the test).
- [ ] **Step 2–4:** Fail → implement → green.
- [ ] **Step 5:** Commit (`feat: runtime configuration accessor`).

---

### Task 2: Filesystem Watcher with stability detection

**Files:** `lib/sync_set/watcher.ex`, `test/sync_set/watcher_test.exs`.

**What you're building:** a GenServer wrapping `file_system` that turns raw FS events into **"stable change at path"** / **"path deleted"** messages sent to a subscriber. Stability avoids hashing half-written files.

**Concepts/APIs:** add `{:file_system, "~> 1.0"}`; `FileSystem.start_link(dirs: [dir])` + `FileSystem.subscribe/1`; receive `{:file_event, watcher_pid, {path, events}}` and `{:file_event, watcher_pid, :stop}`. GenServer with `Process.send_after/3` for debounce timers; track a per-path timer in state. **Put `file_system` behind a thin adapter** (a behaviour or an injected module/pid) so tests can feed synthetic events without touching the disk.

**Stability pseudocode:**
```
on raw event for path:
  (re)schedule a debounce timer for path (e.g. 500ms), cancel any prior one

on debounce fire for path:
  if File.exists?(path):
    stat it; if size+mtime unchanged vs last sample -> emit {:stable, path}
    else resample: store size+mtime, reschedule once more
  else:
    emit {:deleted, path}
```

**Concept to brush up:** debouncing with `Process.send_after` + cancel; why editors emit temp-write + rename and how the final rename looks like a create.

- [ ] **Step 1: Failing tests** (drive the adapter seam, no real sleeps): a burst of events for one path collapses into a single `{:stable, path}`; a path that vanishes emits `{:deleted, path}`; two different paths don't interfere. One separate, tagged smoke test against a real temp dir.
- [ ] **Step 2–4:** Fail → implement → green.
- [ ] **Step 5:** Commit (`feat: filesystem watcher with stability detection`).

---

### Task 3: LocalChange — local edits into the Index, with echo suppression

**Files:** `lib/sync_set/local_change.ex`, `test/sync_set/local_change_test.exs`.

**What you're building:** the glue that reacts to Watcher messages: on `{:stable, path}` checksum the file and compare to the `Index`; on `{:deleted, path}` record a delete. Emit an "announce this change" instruction to the Control plane (Task 4) — but **suppress echoes**.

**Concepts/APIs:** GenServer subscribed to the Watcher; `SyncSet.Checksum.of_file/1`; `SyncSet.Index.get/2`, `local_write/4`, `local_delete/2` **[CONFIRM vs Plan 1]**. Inject the Control process (pid/name) so it's testable in isolation.

**Echo-loop killer pseudocode:**
```
on {:stable, path}:
  {:ok, cksum} = Checksum.of_file(path)
  case Index.get(path):
    %Entry{checksum: ^cksum} -> ignore        # this write is the result of replication
    _ ->
      Index.local_write(path, cksum, size)
      Control.announce(path)                  # broadcast metadata

on {:deleted, path}:
  case Index.get(path):
    %Entry{deleted: false} ->
      Index.local_delete(path)
      Control.announce(path)
    _ -> ignore
```
Key invariant (from the spec): replicated writes update the `Index` **before** writing the file, so the watcher event they cause matches the `Index` checksum and is dropped here.

- [ ] **Step 1: Failing tests:** a stable change with a new checksum → `Index.local_write` called + announce emitted; a stable change whose checksum already equals the Index → **no write, no announce** (echo dropped); a delete of a live file → `local_delete` + announce; a delete of an already-deleted/absent path → nothing. Use a real (temp-file-backed) or faked Index and a stub Control collector.
- [ ] **Step 2–4:** Fail → implement → green.
- [ ] **Step 5:** Commit (`feat: local change handler with echo-loop suppression`).

---

### Task 4: Control plane over Erlang distribution

**Files:** `lib/sync_set/control.ex`, `test/sync_set/control_test.exs`.

**What you're building:** the GenServer that exchanges small control messages between nodes: `announce(path, vector, checksum)`, `want(path)`, and `index_exchange`. It reads the `Index` to build announces and reacts to incoming ones by deciding whether to pull.

**Concepts/APIs:** register the GenServer under a fixed name on every node; send to a peer with `GenServer.cast({__MODULE__, node}, msg)` or `send({name, node}, msg)`; enumerate peers with `Node.list/0`. **Never send file bytes here** — only metadata. To pull, hand off to the DataPlane (Task 5). `SyncSet.Index.snapshot/1`, `get/2`, `apply_remote/3` **[CONFIRM vs Plan 1]**.

**Message handling pseudocode:**
```
announce(path): read Index.get(path) -> broadcast {:announce, path, entry.vector, entry.checksum, self_node} to Node.list()

on {:announce, path, vector, checksum, from_node}:
  compare incoming vector to Index.get(path).vector (or absent):
    incoming dominates / concurrent and we lack the bytes -> DataPlane.pull(from_node, path, expected_checksum)
    a tombstone announce (checksum == nil) that dominates -> Index.apply_remote(path, tombstone_entry); delete local file
    otherwise -> ignore

on {:want, path, reply_to}:  -> DataPlane.serve(reply_to, path)   # actually handled on the data plane; control just routes the request
```
Keep the **conflict/dominance decision in `Replica`/`Index`** (Plan 1). Control only decides *whether to ask for bytes*; once bytes arrive, the receiver calls `Index.apply_remote` which runs the real resolution. **[CONFIRM vs Plan 1]** whether announce carries the full `Entry` (simpler) vs. just vector+checksum — prefer sending the whole `Entry` so the receiver can `apply_remote` directly after fetching bytes.

**Concept to brush up:** Erlang distribution basics — `Node.connect`, shared cookie, `{name, node}` addressing, and **why bulk data over distribution head-of-line-blocks the node** (the reason the data plane is separate).

- [ ] **Step 1: Failing tests:** drive `handle_info`/`handle_cast` directly with crafted messages (no real cluster yet). An announce we already dominate → no pull requested; an announce that dominates us → a pull is requested (assert against a stub DataPlane); a dominating tombstone announce → `apply_remote` + local file delete requested.
- [ ] **Step 2–4:** Fail → implement → green.
- [ ] **Step 5:** Commit (`feat: control plane announce/want over erlang distribution`).

---

### Task 5: Data plane over TCP (transfer + verify + atomic rename)

**Files:** `lib/sync_set/data_plane.ex`, `lib/sync_set/transfer_supervisor.ex`, `test/sync_set/data_plane_test.exs`.

**What you're building:** a TCP listener that serves file bytes on request, and a client that pulls a file, verifies its checksum, and lands it atomically. Each transfer is an isolated task under a `DynamicSupervisor`.

**Concepts/APIs:** `:gen_tcp.listen/2` (`[:binary, packet: 4, active: false, reuseaddr: true]`), `:gen_tcp.accept/1`, an acceptor loop spawning a handler per connection; `:gen_tcp.recv/2`, `:gen_tcp.send/2`; `File.stream!/3` or chunked `:file.read` for sending; `File.open/2` + write to a `.tmp` then `File.rename/2` (atomic on same filesystem); `SyncSet.Checksum.of_file/1` to verify; `DynamicSupervisor` + `Task`/transient children for `TransferSupervisor`.

**Protocol pseudocode (simple length-prefixed):**
```
client pull(node, path, expected_checksum):
  connect to node's data_port
  send {:get, path}
  stream chunks into "<path>.tmp"
  on EOF: cksum = Checksum.of_file(tmp)
    cksum == expected -> Index.apply_remote(path, entry) THEN File.rename(tmp, path)   # Index first => echo suppressed
    cksum != expected -> File.rm(tmp); return {:error, :checksum_mismatch}             # bounded retry by caller/Reconciler

server on {:get, path}: stream the file's bytes back in chunks, then close
```
**Edge cases to honor (spec §error handling):** checksum mismatch → discard `.tmp`, no rename, no Index change; crash mid-transfer is isolated by `TransferSupervisor`; **sweep orphan `*.tmp`** (in `sync_dir`, not in `Index`) on startup; **Index updated before the rename** so the resulting watcher event is dropped by Task 3.

**Concept to brush up:** `:gen_tcp` active vs passive mode and `packet: 4` framing; why `File.rename` is the atomicity primitive; `DynamicSupervisor` restart strategies for transient tasks.

- [ ] **Step 1: Failing tests:** loopback transfer over `127.0.0.1` — serve a temp file, pull it on the same node, assert bytes + checksum match and it lands at the target path; a forced checksum mismatch leaves no file at the target and removes the `.tmp`; orphan-`.tmp` sweep removes a stray temp not in the Index.
- [ ] **Step 2–4:** Fail → implement → green.
- [ ] **Step 5:** Commit (`feat: tcp data plane with checksum-verified atomic transfer`).

---

### Task 6: Reconciler — anti-entropy on (re)connect

**Files:** `lib/sync_set/reconciler.ex`, `test/sync_set/reconciler_test.exs`.

**What you're building:** the component that makes correctness independent of who was online at announce time. On node-up, exchange full indexes, compute what each side is missing, and pull/apply.

**Concepts/APIs:** `:net_kernel.monitor_nodes(true)` → receive `{:nodeup, node}` / `{:nodedown, node}`; request the peer's snapshot via Control (`index_exchange`); for each path, `VersionVector.compare` (via `Index.apply_remote` for metadata, then `DataPlane.pull` for any live entry whose bytes you lack). Retry a failed pull against **any** node advertising that version, not just the origin (spec §error handling 5).

**Reconcile pseudocode (per peer):**
```
on {:nodeup, peer}: send index_exchange request; peer replies with its snapshot
on peer snapshot:
  for {path, their_entry} in snapshot:
    Index.apply_remote(path, their_entry)        # metadata convergence (tombstones, conflicts)
    if resulting local entry is live and we lack the bytes for its checksum:
      DataPlane.pull(some_node_having_it, path, checksum)
  (optionally push: send our snapshot so the peer pulls what IT lacks)
```
Apply metadata first (so tombstones/conflicts resolve), then fetch bytes only for live entries you don't already have.

**Concept to brush up:** anti-entropy vs. gossip/broadcast; why a join-semilattice merge (Plan 1's `Replica.merge`) makes repeated reconciliation safe and convergent.

- [ ] **Step 1: Failing tests:** with stub Control/DataPlane, feed a peer snapshot containing (a) a dominating live entry → `apply_remote` + a pull requested; (b) a dominating tombstone → `apply_remote`, no pull; (c) an entry we already dominate → nothing. A simulated `:nodedown` then `:nodeup` triggers a fresh exchange.
- [ ] **Step 2–4:** Fail → implement → green.
- [ ] **Step 5:** Commit (`feat: reconciler anti-entropy on node up`).

---

### Task 7: Membership via libcluster (Gossip + static fallback)

**Files:** `lib/sync_set/membership.ex` (thin), `config/runtime.exs` (topology).

**What you're building:** automatic LAN discovery so nodes connect without a hand-maintained peer list, with a static fallback.

**Concepts/APIs:** add `{:libcluster, "~> 3.3"}`; configure a `topologies` keyword list with `Cluster.Strategy.Gossip` (multicast) as default and `Cluster.Strategy.Epmd` (static `hosts:` list) switchable by config; start `Cluster.Supervisor` in the app tree. Membership itself is mostly libcluster + the `:net_kernel.monitor_nodes` already used by the Reconciler; this module just exposes `peers/0` (= `Node.list/0`) and any logging.

**Concept to brush up:** libcluster strategies, the shared distribution cookie, and why multicast can be blocked on some networks (the reason to retain the Epmd fallback).

- [ ] **Step 1:** Add the dep and topology config. Manual verification: start two named nodes (`iex --sname a` / `--sname b`) and confirm they auto-connect (`Node.list/0` non-empty). Light test: assert `Membership.peers/0` reflects `Node.list/0`.
- [ ] **Step 2:** Commit (`feat: libcluster gossip discovery with static fallback`).

---

### Task 8: Application supervision tree + cold-start directory scan

**Files:** `lib/sync_set/application.ex`, `lib/sync_set/scanner.ex`, `test/sync_set/scanner_test.exs`.

**What you're building:** wire every component into the supervision tree in the right order, and scan the synced directory at startup so pre-existing files become local creations.

**Concepts/APIs:** `Supervisor` child specs and **start order** (Index and DataPlane listener before Watcher/LocalChange so events have somewhere to go; Cluster.Supervisor for libcluster; Reconciler last). Choose strategy (`:one_for_one` is fine; consider `:rest_for_one` where a downstream depends on an upstream). For the scan: `File.ls!`/`Path.wildcard`, `Checksum.of_file`, compare to `Index.snapshot`, and for any file not in the Index call `Index.local_write` then announce (spec §cold start).

**Scan pseudocode:**
```
on boot, after Index is up:
  sweep orphan *.tmp                       # from Task 5
  for each file under sync_dir:
    {:ok, cksum} = Checksum.of_file(file)
    case Index.get(file):
      %Entry{checksum: ^cksum} -> skip      # already known
      _ -> Index.local_write(file, cksum, size); Control.announce(file)
```

**Concept to brush up:** supervision strategies and child ordering; designing start order around data dependencies.

- [ ] **Step 1: Failing tests** for the Scanner (against a temp dir + faked Index/Control): a brand-new file → `local_write` + announce; a file already in the Index with the same checksum → skipped.
- [ ] **Step 2–4:** Fail → implement Scanner → green; then wire `application.ex` and confirm `mix run --no-halt` (or `iex -S mix`) boots a single node cleanly.
- [ ] **Step 5:** Commit (`feat: supervision tree wiring and cold-start directory scan`).

---

### Task 9: Multi-node integration tests (the real thing)

**Files:** `test/sync_set/integration_test.exs`, test support helpers.

**What you're building:** end-to-end tests across real BEAM nodes over real distribution + TCP, exercising the full spec scenarios.

**Concepts/APIs:** `local_cluster` (`{:local_cluster, "~> 2.0", only: [:test]}`) or the built-in `:peer` module to spawn nodes in-test; per-node temp sync dirs; start the app (or the relevant subtree) on each node with distinct ports; `:rpc`/`:erpc` or `Node.spawn` to write files on a specific node; poll with a bounded wait helper for replication to settle (avoid fixed sleeps).

**Scenarios to cover (map to spec flows):**
```
create -> propagate:        write on A; assert file appears byte-identical on B and C
overwrite -> propagate:     change on A; assert B/C converge to the new checksum
delete -> tombstone:        delete on A; assert B/C remove it and DON'T resurrect it
concurrent -> conflict copy: partition (or near-simultaneous) edits on A and B;
                             assert both end with canonical + sync-conflict copy, identical everywhere
offline -> reconcile:       stop B; change on A; restart B; assert B catches up via anti-entropy
```

**Concept to brush up:** distributed ExUnit (`@tag :distributed`), node startup/teardown, and writing deterministic waits (poll a condition with a timeout) instead of `:timer.sleep`.

- [ ] **Step 1:** Build the node-spawning test harness + a `wait_until/2` helper.
- [ ] **Step 2:** Write the five scenarios as failing tests.
- [ ] **Step 3:** Run; debug real integration issues with the systematic-debugging skill (expect port/cookie/timing wrinkles).
- [ ] **Step 4:** Green; run the full `mix test`.
- [ ] **Step 5:** Commit (`test: multi-node integration tests over distribution and tcp`).

---

## Plan 2 complete — what exists then

A running, masterless, LAN-discovered sync node: edits in a local folder propagate to peers, deletes tombstone correctly, concurrent edits become keep-both conflict copies, and offline nodes catch up via anti-entropy on reconnect — all verified by multi-node integration tests.

**Deferred to future iterations (spec §open items):** first-class renames (lineage-preserving), encryption/auth, block/delta transfer + resume, compression.

---

## Self-Review (against the spec)

**Spec coverage owned by Plan 2:** control plane over Erlang dist (Task 4 ✓); data plane over TCP + checksum verify + atomic rename + `.tmp` sweep (Task 5 ✓); echo-loop fix (Task 3, Index-before-write invariant ✓); Watcher + stability (Task 2 ✓); anti-entropy on reconnect (Task 6 ✓); membership/libcluster Gossip + Epmd fallback (Task 7 ✓); supervision wiring + cold-start scan (Task 8 ✓); split-brain handled via anti-entropy (Tasks 6 + 9 ✓); all five spec message-flows tested (Task 9 ✓).

**Placeholder/dependency notes:** points needing confirmation against the as-built Plan 1 are marked `[CONFIRM vs Plan 1]` (node-id atom, Index/Replica arities, whether announces carry the full `Entry`). These are deliberate — resolve them while implementing, since they depend on Plan 1's final shape.

**No code bodies prescribed** — concepts, APIs, and pseudocode only; the human writes the implementation and tests.
