defmodule SyncSet.IndexTest do
  use ExUnit.Case

  @moduletag :tmp_dir

  alias SyncSet.{Entry, Index}

  setup %{tmp_dir: tmp_dir} do
    # Generate unique names for each test run
    uniq = System.unique_integer([:positive])
    # DETS file lives under ExUnit's tmp_dir, which is wiped before each test
    # so leftover files can never leak across runs
    path = Path.join(tmp_dir, "idx_#{uniq}.dets")
    table = String.to_atom("table_#{uniq}")
    name = String.to_atom("name_#{uniq}")
    opts = [dets_path: path, table: table, name: name]
    start_supervised!({Index, opts})
    {:ok, server: name, opts: opts}
  end

  test "local_write then get -> entry exists with the vector bumped for the node", %{
    server: server
  } do
    path = "/some/path/to/file.txt"
    :ok = Index.local_write(server, path, "asdf1234", 1)
    entry = Index.get(server, path)
    # The first write sets component version to 1
    assert entry.vector == %{nonode@nohost: 1}
    :ok = Index.local_write(server, path, "bsdf1234", 1)
    entry = Index.get(server, path)
    # A subsequent write sets component version to 2
    assert entry.vector == %{nonode@nohost: 2}
  end

  test "snapshot -> returns all written paths", %{server: server} do
    path = "/some/path/to/file0.txt"
    :ok = Index.local_write(server, path, "asdf1234", 1)
    path = "/some/path/to/file1.txt"
    :ok = Index.local_write(server, path, "asdf1235", 1)
    path = "/some/path/to/file2.txt"
    :ok = Index.local_write(server, path, "asdf1236", 1)
    index = Index.snapshot(server)

    assert index == %{
             "/some/path/to/file0.txt" => %SyncSet.Entry{
               checksum: "asdf1234",
               deleted: false,
               size: 1,
               vector: %{nonode@nohost: 1}
             },
             "/some/path/to/file1.txt" => %SyncSet.Entry{
               checksum: "asdf1235",
               deleted: false,
               size: 1,
               vector: %{nonode@nohost: 1}
             },
             "/some/path/to/file2.txt" => %SyncSet.Entry{
               checksum: "asdf1236",
               deleted: false,
               size: 1,
               vector: %{nonode@nohost: 1}
             }
           }
  end

  test "local_delete -> entry is now a tombstone", %{server: server} do
    path = "/some/path/to/file.txt"
    :ok = Index.local_write(server, path, "asdf1234", 1)
    entry = Index.get(server, path)
    # The first write sets component version to 1
    assert entry.vector == %{nonode@nohost: 1}
    :ok = Index.local_delete(server, path)
    entry = Index.get(server, path)

    assert entry == %Entry{
             vector: %{nonode@nohost: 2},
             checksum: nil,
             deleted: true,
             size: 0
           }
  end

  test "apply_remote -> a remote entry lands in the index", %{server: server} do
    path0 = "/some/path/to/file0.txt"
    path1 = "/some/path/to/file1.txt"

    :ok = Index.local_write(server, path0, "asdf1234", 1)

    # Apply remote entry for untracked file
    :ok =
      Index.apply_remote(server, path1, %Entry{
        vector: %{nonode@nohost: 1, somenode@nohost: 2},
        checksum: "sdfg2345",
        deleted: false,
        size: 1
      })

    assert Enum.sort(Map.keys(Index.snapshot(server))) == Enum.sort([path0, path1])

    # Apply dominating entry for tracked file
    :ok =
      Index.apply_remote(server, path0, %Entry{
        vector: %{nonode@nohost: 2, somenode@nohost: 3},
        checksum: "asdf1234",
        deleted: false,
        size: 1
      })

    # Dominating entry overwrites file0 entry
    assert Index.snapshot(server) == %{
             "/some/path/to/file0.txt" => %SyncSet.Entry{
               checksum: "asdf1234",
               deleted: false,
               size: 1,
               vector: %{nonode@nohost: 2, somenode@nohost: 3}
             },
             "/some/path/to/file1.txt" => %SyncSet.Entry{
               checksum: "sdfg2345",
               deleted: false,
               size: 1,
               vector: %{nonode@nohost: 1, somenode@nohost: 2}
             }
           }

    # Apply concurrent entry for tracked file
    {:conflict, _, :local} =
      Index.apply_remote(server, path0, %Entry{
        vector: %{nonode@nohost: 3, somenode@nohost: 2},
        checksum: "dfgh3456",
        deleted: false,
        size: 1
      })

    # Concurrent entry is resolved for file0 
    assert Index.snapshot(server)[path0].vector == %{nonode@nohost: 3, somenode@nohost: 3}

    assert Enum.sort(Map.keys(Index.snapshot(server))) ==
             Enum.sort([
               "/some/path/to/file0.sync-conflict-asdf1234.txt",
               path0,
               path1
             ])
  end

  test "persistence across restart", %{server: server, opts: opts} do
    path = "/some/path/to/file.txt"
    :ok = Index.local_write(server, path, "asdf1234", 1)
    before_snapshot = Index.snapshot(server)

    assert before_snapshot == %{
             "/some/path/to/file.txt" => %SyncSet.Entry{
               checksum: "asdf1234",
               deleted: false,
               size: 1,
               vector: %{nonode@nohost: 1}
             }
           }

    stop_supervised!(Index)
    start_supervised!({Index, opts})
    after_snapshot = Index.snapshot(server)
    assert after_snapshot == before_snapshot
  end
end
