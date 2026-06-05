# Replicated Folder Sync — Design (Iteration 1)

**Date:** 2026-06-04
**Status:** Approved design, pre-implementation
**Purpose:** Learning project. Engage honestly with three layers — distributed
Elixir/OTP, distributed-systems theory, and filesystem/IO plumbing. Jank is
acceptable where explicitly scoped out; correctness of the core model is not.

## Naming

The original working title "shared drive" is misleading — that implies a mounted
remote filesystem (NFS/SMB). This is **file synchronization / replication**: every
machine keeps its own local copy and changes propagate. Closest prior art:
**Syncthing** (peer-to-peer, masterless, LAN-friendly).

Domain noun: a **replicated folder** (a.k.a. "sync set"). Project name TBD by the
user; candidates: `Replica`, `Mesh`, `LanSync`, `Driftwood`. Not blocking.

## Core model

- **Masterless full mesh.** Every node runs an identical stack. No master, no
  central registry. The earlier master/worker idea is dropped: the master was
  implicitly serializing writes and maintaining a "who has which version"
  registry; in the masterless model that state lives as each node's own local
  Index plus what it learns from announces and anti-entropy.
- **Two planes:**
  - **Control plane** — small messages over **Erlang distribution** (announces,
    version vectors, anti-entropy index exchange). Leans into the OTP learning goal.
  - **Data plane** — file bytes over a **separate plain TCP socket** per node.
    Bulk data never touches the distribution channel (avoids head-of-line
    blocking / node stalls).
- **Consistency:** eventual consistency with **version vectors**. Conflicts are
  *detected*, not guessed. Wall-clock time is never used for correctness.
- **Convergence invariant:** any two nodes that have exchanged the same set of
  updates end up with byte-identical synced directories, including which
  `sync-conflict` copies exist.

## Scope (Iteration 1)

**In scope:**
- Operations: **create, overwrite, delete** (delete via tombstones).
- Version vectors with conflict detection → `sync-conflict` copies.
- Anti-entropy reconciliation on node (re)connect.
- Whole-file transfer over TCP with checksum verification.
- Cold start: scan existing local files and index them as local creations.
- Node membership via **libcluster Gossip** (LAN multicast), with static `Epmd`
  strategy retained as a config-switchable fallback.

**Explicitly out of scope (deferred, not forgotten):**
- Renames as first-class moves (iteration 1 sees them as delete + create).
- Encryption / authentication (assumes a trusted LAN).
- Large-file delta/block transfer (we send whole files).
- Partial-file resume, compression.

## Components (per node — identical on every node)

Supervision tree under a top supervisor. Each unit has one job, a clear
interface, and is independently testable.

| Component | One job | Depends on |
|---|---|---|
| **Index** (GenServer + DETS) | Authoritative local map `path → {version_vector, checksum, size, deleted?}`. Persisted across restarts. | — |
| **Watcher** | Wrap `file_system`; debounce; wait for file *stability*; emit "stable change at path". Behind a thin adapter so tests can inject synthetic events. | filesystem |
| **LocalChange** | On a stable change: checksum it, compare to Index. **If checksum already equals Index → ignore (echo-loop killer).** Else bump this node's vector component, update Index, hand to Control. | Index, Watcher |
| **Membership** | Wraps libcluster node up/down events; exposes current peer set. | libcluster |
| **Control** (GenServer over Erlang dist) | Send/receive `announce(path, vector, checksum)`, `want(path)`, and `index_exchange` (anti-entropy). | Membership, Index |
| **DataPlane** | TCP listener + sender. On `want`, stream file in chunks; receiver verifies checksum, writes to temp, **atomic rename** into place (Index updated *first*). | Index |
| **Reconciler** | On node-up: exchange indexes, compare version vectors (dominates / dominated / concurrent → conflict-copy), apply tombstones, pull/push deltas. Retries against *any* node advertising a needed version. | Control, Index, DataPlane |
| **TransferSupervisor** (DynamicSupervisor) | Isolates each transfer task so a mid-transfer crash takes nothing else down. | — |

### The echo-loop fix (structural, not timing-based)

The Index is the single source of truth for "current version of each path."
Replicated writes update the Index **before** touching the file, and
`LocalChange` ignores any watcher event whose checksum already equals the Index.
No suppression windows, no races.

## Message flows

### (a) Create / overwrite — happy path
1. User saves `report.txt` on **A**. Watcher fires; LocalChange waits for
   stability, checksums → `h1`.
2. Checksum differs from Index → A bumps its component: `report.txt {A:1}`,
   stores `{vector, h1, size}` in Index.
3. A's Control broadcasts `announce("report.txt", {A:1}, h1)` to peers.
4. B receives announce; incoming `{A:1}` dominates its (absent) entry → B records
   the metadata and sends `want("report.txt")` to A.
5. A's DataPlane streams chunks over TCP. B writes `report.txt.tmp`, verifies
   checksum == `h1`. **Index updated to `{A:1}, h1` first**, then atomic rename.
6. B's Watcher fires → LocalChange checksums → matches Index → **ignored.** No
   echo. C does the same independently.

### (b) Concurrent conflict
1. State everywhere: `report.txt {A:1}`.
2. Near-simultaneous / partitioned: A edits → `{A:2}`; B edits → `{A:1, B:1}`.
3. When the announces meet, B compares incoming `{A:2}` to local `{A:1, B:1}`:
   neither dominates → **concurrent.**
4. Deterministic resolution (identical on every node — e.g. lower node-id keeps
   the canonical name): keep A's content as `report.txt`, write B's as
   `report.sync-conflict-<B>-<ts>.txt`, and **merge vectors** → `{A:2, B:1}` so
   the resolution converges and doesn't re-trigger. Timestamp in the filename is
   cosmetic only.

**Conflicts are keep-both, never content-merge.** Iteration 1 does *no*
content-aware merging (no three-way merge, no combining lines). When two versions
are concurrent, both whole files are preserved: one keeps the canonical name, the
other is saved under a `sync-conflict` name. Combining their contents into a
single file is a manual step left to the human.

**Both files exist on every node.** Resolution converges to a byte-identical
directory everywhere — A, B, and any uninvolved node C all end up holding *both*
`report.txt` and `report.sync-conflict-<B>-<ts>.txt`. The losing edit is not
"owned" by or confined to node B; it became a new file that replicates to
everyone like any other. The `-<B>-` in the name only records where the losing
edit *originated*, not where it lives. (If nodes kept the same content under
different names, the directories would disagree forever and never converge —
which is exactly what this rule prevents.)

**The deterministic rule** decides only one thing: *which concurrent version
keeps the original filename.* It must be a pure function of data both nodes
already hold (the two vectors, checksums, node-ids) — never wall-clock time or
announce-arrival order, since those differ per node and would break convergence.
"Lower node-id keeps the name" is one valid rule; "larger checksum wins" or
"higher node-id wins" work equally well. Both files survive regardless of the
rule chosen.

### (c) Delete with tombstone
1. State: `report.txt {A:2, B:1}` everywhere.
2. C deletes it → tombstone `report.txt {A:2, B:1, C:1}, deleted: true`. Index
   keeps the (tombstone) entry; file removed locally.
3. C announces the tombstone. B sees it dominates `{A:2, B:1}` → B deletes its
   copy, stores the tombstone.
4. **Resurrection trap averted:** D was offline holding live `{A:2, B:1}`. On
   reconnect, Reconciler exchanges indexes; the tombstone dominates D's version →
   D deletes, rather than D's live copy resurrecting the file everywhere.

## Error handling & edge cases

1. **File still being written when watcher fires** — Watcher stability detection:
   wait for a quiet period (~500ms no further events for the path) and confirm
   size+mtime unchanged across two reads before checksumming. Editor temp-write +
   rename produces a final rename event treated as the real one.
2. **Checksum mismatch after transfer** — discard `.tmp`, no rename, no Index
   change; re-issue `want` (bounded retries). File stays un-replicated until a
   clean transfer or next anti-entropy pass. The atomic rename only happens
   post-verification, so a corrupt file never lands in place.
3. **Node crashes mid-transfer** — each transfer is a task under
   `TransferSupervisor`; its crash is isolated. Orphan `.tmp` files (in dir, not
   in Index) are swept on startup. Pull is retried; idempotent because the Index
   drives "do I still need this?"
4. **Source file changes again during transfer** — B lands `{A:1}`, then A's
   `{A:2}` announce arrives → B pulls again. Always converges; a wasted transfer
   is acceptable jank.
5. **Source drops before serving `want`** — Reconciler retries against any node
   advertising that version on the next index exchange, not just the origin.
6. **Split brain / reconnect** — no special handling; anti-entropy index exchange
   on heal sorts dominance vs. concurrency exactly as in the live path.
   Independently-arisen conflicts surface as `sync-conflict` copies. Expected
   behavior, not an error.
7. **Clock issues** — version vectors mean wall-clock is never used for
   correctness. Timestamps appear only in conflict-copy filenames (cosmetic).
8. **Cold start with non-empty dir** — scan the dir; any file not in the
   persisted Index becomes a local create (`{self:1}`) and is announced.

### Re-creating a path that has a tombstone (load-bearing rule)

**A create/overwrite bumps its vector from whatever is currently in the Index for
that path — including a tombstone.** When a file is created at a path that holds a
`deleted: true` tombstone, the new entry increments the local component *past* the
tombstone's vector, so the live version **dominates** the tombstone and resurrects
the file everywhere.

Without this rule, a re-created file could receive a vector that is *dominated by*
the old tombstone, and the resurrection-prevention logic (see flow (c)) would
delete the new file right back. "Creates bump from the existing entry, tombstone
or not" is what lets a path legitimately come back to life.

**This is also how renames work in iteration 1** (renames are out of scope only as
a *first-class move*). The watcher sees a rename as a delete of the old path plus a
create of the new path. Example — resolving a conflict by promoting the
sync-conflict copy on node C:

1. Delete `notes.txt` (apples) → tombstone `{A:2, B:1, C:1}`; propagates, all
   nodes delete their copy.
2. Rename `notes.sync-conflict-B-<ts>.txt` → `notes.txt`. Filesystem emits two
   events:
   - Delete of the conflict path → tombstone → conflict file removed everywhere.
   - Create of `notes.txt` (oranges) → bumps from the existing tombstone to
     `{A:2, B:1, C:2}`, live, which dominates the tombstone → every node
     resurrects `notes.txt` with the oranges content.

End state everywhere: just `notes.txt` (oranges). The only cost of deferring
first-class renames is **no lineage** (the new file isn't recorded as descending
from the conflict file) and **wasted bytes** (oranges content is re-transferred as
a fresh file rather than recognized as the same bytes under a new path). Neither
affects correctness.

## Testing strategy

- **Version-vector algebra — property-based (`StreamData`).** Pure functions:
  `compare/2` → `:dominates | :dominated | :concurrent | :equal`, `merge/2`,
  `increment/2`. Properties: merge commutative & idempotent; increment always
  dominates. No I/O.
- **Conflict resolution determinism** — same pair of concurrent versions →
  identical winner + conflict-copy name regardless of node or arrival order.
- **Convergence property test (crown jewel)** — simulate N in-memory nodes, apply
  a random op sequence delivered in random order with drops/dupes, run
  anti-entropy to quiescence, assert all nodes byte-identical. The definitive
  eventual-consistency test.
- **Index store** — GenServer/DETS round-trip and persistence-across-restart.
- **Multi-node integration** — real BEAM nodes in-test (`:peer` module or
  `local_cluster`) exercising create → propagate, concurrent → conflict-copy,
  delete → tombstone, offline → reconcile-on-rejoin over real distribution + TCP.
- **Watcher** — thin adapter lets tests inject synthetic FS events; one smoke
  test against a real temp dir.

Clean boundaries pay off: vector algebra, conflict resolution, and reconciliation
are pure or near-pure and testable without network or filesystem; only DataPlane
and Watcher touch the messy edges.

## Open items for future iterations

- Project naming decision.
- Renames as first-class moves (preserve version history across path change).
- Security: encryption + authentication for untrusted networks.
- Efficiency: block/delta transfer, resume, compression.
