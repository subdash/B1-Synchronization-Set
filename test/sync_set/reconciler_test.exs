defmodule SyncSet.ReconcilerTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir
  @moduletag :capture_log
  alias SyncSet.{Conflict, Entry, Index, Reconciler, Checksum}

  describe "reconcile_snapshot decision logic" do
    setup %{tmp_dir: tmp_dir} do
      uniq = System.unique_integer([:positive])
      sync_dir = Path.join(tmp_dir, "sync_dir")
      :ok = File.mkdir_p!(sync_dir)

      index_name = String.to_atom("reconciler_index_#{uniq}")

      index_opts = [
        dets_path: Path.join(tmp_dir, "idx_#{uniq}.dets"),
        table: String.to_atom("reconciler_table_#{uniq}"),
        name: index_name
      ]

      start_supervised!({Index, index_opts})

      state = %{index: index_name, sync_dir: sync_dir}
      # Instead of opening a socket, message the test process. Tests then
      # assert_received/refute_received {:pull_requested, path}.
      test_pid = self()

      transfer_fn = fn _peer, path, _entry ->
        send(test_pid, {:pull_requested, path})
        {:ok, :stubbed}
      end

      %{
        index: index_name,
        sync_dir: sync_dir,
        state: state,
        transfer_fn: transfer_fn
      }
    end

    test "dominating live entry -> apply + pull", %{
      index: index_name,
      state: state,
      transfer_fn: transfer_fn
    } do
      path = "file.txt"

      entry = %Entry{
        checksum: "abcdef12",
        deleted: false,
        size: 8,
        vector: %{nonode@nohost: 1}
      }

      snapshot = %{"file.txt" => entry}

      Reconciler.reconcile_snapshot(snapshot, ~c"127.0.0.1", 12345, state, transfer_fn)

      assert entry == Index.get(index_name, path)
      assert_received {:pull_requested, ^path}
    end

    test "dominating tombstone -> apply, no pull", %{
      index: index_name,
      state: state,
      transfer_fn: transfer_fn
    } do
      path = "file.txt"

      entry = %Entry{
        checksum: nil,
        deleted: true,
        size: 0,
        vector: %{nonode@nohost: 1}
      }

      snapshot = %{"file.txt" => entry}

      Reconciler.reconcile_snapshot(snapshot, ~c"127.0.0.1", 12345, state, transfer_fn)

      assert entry == Index.get(index_name, path)
      refute_received {:pull_requested, ^path}
    end

    test "already dominated -> nothing", %{
      index: index_name,
      sync_dir: sync_dir,
      state: state,
      transfer_fn: transfer_fn
    } do
      rel_path = "file.txt"
      abs_path = Path.join(sync_dir, rel_path)
      :ok = File.write!(abs_path, "some content")
      {:ok, checksum} = Checksum.of_file(abs_path)
      Index.local_write(index_name, rel_path, checksum, 8)
      :ok = File.write!(abs_path, "more content")
      {:ok, checksum} = Checksum.of_file(abs_path)
      Index.local_write(index_name, rel_path, checksum, 8)

      expected = %Entry{
        checksum: checksum,
        deleted: false,
        size: 8,
        vector: %{nonode@nohost: 2}
      }

      entry = Index.get(index_name, rel_path)
      assert entry == expected

      # Incoming snapshot comes in with the checksum, but is dominated by the local
      incoming_snapshot = %{
        rel_path => %{
          checksum: checksum,
          deleted: false,
          size: 8,
          vector: %{nonode@nohost: 1}
        }
      }

      Reconciler.reconcile_snapshot(
        incoming_snapshot,
        ~c"127.0.0.1",
        checksum,
        state,
        transfer_fn
      )

      # Local entry remains the same (incoming was dominated)
      entry = Index.get(index_name, rel_path)
      assert entry == expected
      # Hence, no pull requested
      refute_received {:pull_requested, ^rel_path}
    end

    test "live entry we have bytes for -> no pull", %{
      index: index_name,
      sync_dir: sync_dir,
      state: state,
      transfer_fn: transfer_fn
    } do
      rel_path = "file.txt"
      abs_path = Path.join(sync_dir, rel_path)
      :ok = File.write!(abs_path, "some content")
      {:ok, checksum} = Checksum.of_file(abs_path)
      Index.local_write(index_name, rel_path, checksum, 8)

      expected = %Entry{
        checksum: checksum,
        deleted: false,
        size: 8,
        vector: %{nonode@nohost: 1}
      }

      entry = Index.get(index_name, rel_path)
      assert entry == expected

      # Incoming snapshot comes in with the checksum, but is dominated by the local
      incoming_snapshot = %{
        rel_path => %Entry{
          checksum: checksum,
          deleted: false,
          size: 8,
          vector: %{nonode@nohost: 2}
        }
      }

      Reconciler.reconcile_snapshot(
        incoming_snapshot,
        ~c"127.0.0.1",
        checksum,
        state,
        transfer_fn
      )

      # Local entry remains the same (incoming was dominated)
      entry = Index.get(index_name, rel_path)
      expected = %{entry | vector: %{nonode@nohost: 2}}
      assert entry == expected
      # Hence, no pull requested
      refute_received {:pull_requested, ^rel_path}
    end

    test "concurrent edit, local loses -> conflict copy written to disk",
         %{
           index: index_name,
           sync_dir: sync_dir,
           state: state,
           transfer_fn: transfer_fn
         } do
      rel_path = "file.txt"
      abs_path = Path.join(sync_dir, rel_path)
      :ok = File.write!(abs_path, "some content")
      {:ok, checksum} = Checksum.of_file(abs_path)
      Index.local_write(index_name, rel_path, checksum, 8)

      incoming_checksum = checksum <> "z"

      incoming_snapshot = %{
        rel_path => %Entry{
          checksum: incoming_checksum,
          deleted: false,
          size: 8,
          vector: %{bronode@nohost: 2}
        }
      }

      Reconciler.reconcile_snapshot(
        incoming_snapshot,
        ~c"127.0.0.1",
        0,
        state,
        transfer_fn
      )

      conflict_path = Conflict.conflict_path(rel_path, checksum)
      abs_conflict_path = Path.join(sync_dir, conflict_path)
      assert File.exists?(abs_conflict_path)
      new_contents = File.read!(abs_conflict_path)
      assert new_contents == "some content"
    end
  end
end
