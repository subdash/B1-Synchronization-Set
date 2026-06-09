# Concepts Covered

A running glossary of terms covered while building `sync_set`. **Terms only, by
design** — explanations live in the code comments, the design spec, the plans,
and our sessions. This is a checklist/index, not a textbook.

## Distributed systems & replication theory

- Masterless / leaderless replication
- Version vector
- Component of a version vector; local component
- Causal comparison outcomes: `dominates`, `dominated`, `equal`, `concurrent`
- Partial order
- Incomparability (concurrency)
- Join-semilattice
- Join / least upper bound (LUB)
- Componentwise max (the join on version vectors)
- Bottom element
- Idempotent
- Commutative
- Associative
- Monotonic (inflationary) updates
- Conflict-free Replicated Data Type (CRDT)
- State-based CRDT — Convergent RDT (CvRDT)
- Operation-based CRDT — Commutative RDT (CmRDT)
- Strong Eventual Consistency (SEC)
- Eventual delivery
- Anti-entropy
- Gossip propagation
- Tombstone
- Resurrection
- Deterministic conflict resolution
- Keep-both resolution
- `sync-conflict` copy / conflict path
- Last-writer-wins (contrast)
- Modify-wins vs delete-wins (concurrent edit/delete policy)
- Content checksum / hash (SHA-256)
- Term ordering as a deterministic tie-break

## Elixir / OTP

- Functional core (pure functions over immutable data)
- Struct (`defstruct`) and struct pattern matching
- `@type` / typespec (e.g. `String.t() | nil`)
- `Map.merge/3` with a resolver function
- `Map.update/4`
- `Map.get/3` with a default
- `Enum.reduce/3` (fold; accumulator threading)
- `with` expression (else-less pass-through of non-matching values)
- `case` / `cond`
- Alias (`alias`, `alias __MODULE__`)
- Module name ↔ file-path naming convention
- `Path`: `extname/1`, `basename/2`, `dirname/1`, `join/2`
- `:crypto.hash/2`, `Base.encode16/2`
- TOCTOU race (`File.exists?` + `File.read!` vs `File.read/1`)
- GenServer (deferred process wrapper around the pure core)
- Compile-time vs runtime module availability
- Elixir set-theoretic type checker (1.20)

## Testing

- ExUnit (`describe`, `test`, `@tag :tmp_dir`)
- TDD rhythm (red → green → commit)
- Property-based testing (StreamData, `ExUnitProperties`)
- `check all` and generators (`map_of`, `member_of`, `integer`, `constant`)
- Generator sizing (`max_length`, key-pool headroom to avoid duplicate-key flakes)
- Shrinking of counterexamples
- Property laws: idempotence, commutativity, associativity, antisymmetry/symmetry
- Convergence property (the eventual-consistency test)

### this needs to be fragmented/moved

    # join-semilattice: a set of elements and a binary operation, join (v), which is:
    # 1. idempotent: a v a = a
    # 2. commutative: a v b = b v a
    # 3. Associative: (a v b) v c = a v (b v c)
    #
    # This introduces a partial order to the elements: a <= b when a v b = b
    #
    # The order the lattice gives us is the compare function:
    # - a v b = b -> a <= b -> compare says :dominated/:equal
    # - a v b = a -> b <= a -> :dominates/:equal
    #
    # Why partial? Because some pairs are incomparable, neither a <= b or b <= a.
    # (Think concurrent updates)
    # The join a v b is the least upper bound (LUB) of a and b: the smallest element
    # that is >= both.
    #
    # This is the crux of the whole sync set design:
    # join is idempotent + commutative + associative.
    # (*) Monotonic: Every merge moves you up the lattice, state never moves backward
    # (*) Order/duplication-proof: you can merge peer states in any order and re-merge
    #     them any number of times, which is important for a gossip network with out
    #     of order duplicate messages
    # (*) Single destination: a group of replicas merging toward each other converges
    #     on one element -- the LUB of every update anyone has seen
    #
    # This enables the data structure we're building, a Conflict Free Replicated Data Type
    # (CRDT). It's a structure designed to be copied across machines, updated independently
    # on each (no locking or coordinator), and then merged back together with a mathematical
    # guarantee that everyone ends up agreeing.
    #
    # Specifically we've built a CvRDT (Convergent RDT -- state based). Replicas sync by
    # shipping their whole state and merging with a join.
    #
    # To build a CvRDT you need:
    # 1. State forms a join-semilattice (version vectors)
    # 2. merge is the join (LUB) - componentwise max
    # 3. Updates are monotonic (inflationary) - every local update moves state strictly
    #    up the lattice
    #
    # This gives us Strong Eventual Consistency (SEC): any two replicas that have received
    # the same set of updates are in the same state, regardless of the order, duplication
    # or delays of the delivery. The only requirement of the network is eventual delivery
    # of updates, even if updates are reordered, duplicated or batched.
    #
    # Our answer to resolve conflicts is deterministic -- to keep both files for concurrent edits.
