defmodule ReplicaTest do
  alias SyncSet.{VersionVector, Replica, Entry}

  use ExUnit.Case

  @node :node1

  describe "Replica tests" do
    test "local_write on fresh path creates vector node with value 1" do
      r = new_replica()
      assert r.index == %{}
      r = write(r, "README", "abcdefgh")
      assert r.index["README"].vector[@node] == 1
    end

    test "local_write increments node for each call" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")
      r = write(r, "README", "abcdefgh")
      assert r.index["README"].vector[@node] == 2
    end

    test "local_delete on a live entry tombstones and bumps vector" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")
      assert r.index["README"].vector[@node] == 1
      r = delete(r, "README")
      entry = r.index["README"]
      assert entry.vector[@node] == 2
      assert entry.deleted == true
    end

    test "local_delete no-op on absent entry" do
      r = new_replica()
      assert r.index["README"] == nil
      r = delete(r, "README")
      assert r.index["README"] == nil
    end

    test "local_delete no-op on already deleted entry" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")
      r = delete(r, "README")
      entry = r.index["README"]
      assert entry.deleted == true
      assert entry.vector[@node] == 2
      r = delete(r, "README")
      entry = r.index["README"]
      assert entry.vector[@node] == 2
    end

    test "apply_remote accepts a dominating entry" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")
      r = write(r, "README", "bcdefghi")

      assert r.index["README"].vector[@node] == 2

      incoming_entry = %Entry{
        vector: %{node1: 3},
        checksum: "cdefghij",
        size: 2,
        deleted: false
      }

      assert VersionVector.compare(incoming_entry.vector, r.index["README"].vector) == :dominates
      r = Replica.apply_remote(r, "README", incoming_entry)

      assert r.index["README"].vector == %{node1: 3}
    end

    test "apply_remote ignores a dominated entry" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")
      r = write(r, "README", "bcdefghi")

      assert r.index["README"].vector[@node] == 2

      incoming_entry = %Entry{
        vector: %{node1: 1},
        checksum: "cdefghij",
        size: 2,
        deleted: false
      }

      assert VersionVector.compare(incoming_entry.vector, r.index["README"].vector) == :dominated
      r = Replica.apply_remote(r, "README", incoming_entry)

      assert r.index["README"].vector == %{node1: 2}
    end

    test "apply_remote applies a dominating tombstone" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")
      r = write(r, "README", "bcdefghi")

      assert r.index["README"].vector[@node] == 2
      assert r.index["README"].deleted == false

      r =
        Replica.apply_remote(r, "README", %Entry{
          vector: %{node1: 3},
          checksum: "cdefghij",
          size: 2,
          deleted: true
        })

      assert r.index["README"].vector == %{node1: 3}
      assert r.index["README"].deleted == true
    end

    test "apply_remote resurrects with a dominated tombstone" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")
      r = delete(r, "README")

      assert r.index["README"].vector[@node] == 2
      assert r.index["README"].deleted == true

      r =
        Replica.apply_remote(r, "README", %Entry{
          vector: %{node1: 3},
          checksum: "cdefghij",
          size: 2,
          deleted: false
        })

      assert r.index["README"].vector == %{node1: 3}
      assert r.index["README"].deleted == false
    end

    test "two concurrent live edits resolve correctly" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")

      assert r.index["README"].vector[@node] == 1

      incoming_entry = %Entry{
        vector: %{node2: 1},
        checksum: "cdefghij",
        size: 2,
        deleted: false
      }

      r = Replica.apply_remote(r, "README", incoming_entry)

      assert r.index ==
               %{
                 "README" => %Entry{
                   # Larger checksum keeps the canonical name
                   checksum: "cdefghij",
                   deleted: false,
                   size: 2,
                   # Larger checksum also contains merged vector
                   vector: %{node2: 1, node1: 1}
                 },
                 "README.sync-conflict-abcdefgh" => %Entry{
                   checksum: "abcdefgh",
                   deleted: false,
                   size: 1,
                   # Conflict file retains the current vector
                   vector: %{node1: 1}
                 }
               }
    end

    test "two concurrent edits where one deletes results in the live entry winning" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")

      assert r.index["README"].vector[@node] == 1

      incoming_entry = %Entry{
        vector: %{node2: 1},
        checksum: "cdefghij",
        size: 2,
        deleted: true
      }

      r = Replica.apply_remote(r, "README", incoming_entry)

      assert r.index ==
               %{
                 "README" => %Entry{
                   checksum: "abcdefgh",
                   deleted: false,
                   size: 1,
                   vector: %{node1: 1, node2: 1}
                 }
               }
    end

    test "apply_remote is idempotent for an incoming entry that dominates" do
      # Applying a entry which dominates does not change the index when applied
      # a second time.
      r = new_replica()
      r = write(r, "README", "abcdefgh")

      assert r.index["README"].vector[@node] == 1

      incoming_entry = %Entry{
        vector: %{node1: 2},
        checksum: "cdefghij",
        size: 2,
        deleted: false
      }

      assert VersionVector.compare(incoming_entry.vector, r.index["README"].vector) ==
               :dominates

      r = Replica.apply_remote(r, "README", incoming_entry)

      current_expected_index = %{"README" => incoming_entry}

      assert current_expected_index == r.index
      # Second apply
      r = Replica.apply_remote(r, "README", incoming_entry)
      assert r.index == current_expected_index
    end

    test "apply_remote is idempotent for an incoming entry that is concurrent" do
      r = new_replica()
      r = write(r, "README", "abcdefgh")

      assert r.index["README"].vector[@node] == 1

      incoming_entry = %Entry{
        vector: %{node2: 3},
        checksum: "cdefghij",
        size: 2,
        deleted: false
      }

      assert VersionVector.compare(incoming_entry.vector, r.index["README"].vector) ==
               :concurrent

      r = Replica.apply_remote(r, "README", incoming_entry)

      current_expected_index = %{
        "README" => %Entry{
          checksum: "cdefghij",
          deleted: false,
          size: 2,
          vector: %{node1: 1, node2: 3}
        },
        "README.sync-conflict-abcdefgh" => %Entry{
          checksum: "abcdefgh",
          deleted: false,
          size: 1,
          vector: %{node1: 1}
        }
      }

      assert current_expected_index == r.index
      # Second apply
      r = Replica.apply_remote(r, "README", incoming_entry)
      assert r.index == current_expected_index
    end
  end

  # --- Test helpers ---------------------------------------------------------

  defp new_replica(node \\ @node), do: Replica.new(node)

  defp write(replica, path, checksum, size \\ 1) do
    {replica, _entry} = Replica.local_write(replica, path, checksum, size)
    replica
  end

  defp delete(replica, path) do
    {replica, _entry} = Replica.local_delete(replica, path)
    replica
  end
end
