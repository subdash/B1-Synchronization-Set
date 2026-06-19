defmodule SyncSet.DataPlaneTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir
  @moduletag :capture_log
  alias SyncSet.{DataPlane, Entry, Index, Checksum, TransferSupervisor}

  # server sync dir -> holds real source file the listener serves
  # client sync dir -> where pull writes tmp files and renames
  setup %{tmp_dir: tmp_dir} do
    uniq = System.unique_integer([:positive])
    # ExUnit's tmp_dir is wiped before each test, so nothing leaks across runs
    # Need separate sync dirs to exercise server vs client
    server_sync_dir = Path.join(tmp_dir, "server_sync_dir")
    client_sync_dir = Path.join(tmp_dir, "client_sync_dir")
    :ok = File.mkdir_p!(server_sync_dir)
    :ok = File.mkdir_p!(client_sync_dir)
    server_index_name = String.to_atom("server_name_#{uniq}")
    client_index_name = String.to_atom("client_name_#{uniq}")
    server_dets_path = Path.join(tmp_dir, "server_idx_#{uniq}.dets")
    client_dets_path = Path.join(tmp_dir, "client_idx_#{uniq}.dets")
    server_dets_table = String.to_atom("server_table_#{uniq}")
    client_dets_table = String.to_atom("client_table_#{uniq}")

    server_idx_opts = [
      dets_path: server_dets_path,
      table: server_dets_table,
      name: server_index_name
    ]

    client_idx_opts = [
      dets_path: client_dets_path,
      table: client_dets_table,
      name: client_index_name
    ]

    data_plane_opts = [port: 0, sync_dir: server_sync_dir, index: server_index_name]

    host = ~c"127.0.0.1"
    start_supervised!({Index, server_idx_opts}, id: :server_index)
    start_supervised!({Index, client_idx_opts}, id: :client_index)
    data_plane_pid = start_supervised!({DataPlane, data_plane_opts})
    %{port: bound_port} = :sys.get_state(data_plane_pid)

    # Write a file to server and add it to index
    test_relative_path = "test_file"
    test_absolute_path = Path.join(server_sync_dir, test_relative_path)
    :ok = File.write!(test_absolute_path, "asdf")
    {:ok, %{size: test_file_size}} = File.stat(test_absolute_path)
    {:ok, test_file_checksum} = Checksum.of_file(test_absolute_path)
    Index.local_write(server_index_name, test_relative_path, test_file_checksum, test_file_size)
    test_entry = Index.get(server_index_name, test_relative_path)

    {:ok,
     client_index_name: client_index_name,
     host: host,
     bound_port: bound_port,
     client_sync_dir: client_sync_dir,
     test_entry: test_entry,
     test_relative_path: test_relative_path,
     test_file_checksum: test_file_checksum}
  end

  # Fills in the per-test connection and client details from the test context,
  # leaving only the file path and entry to vary per call.
  defp pull_file(ctx, file_path, entry) do
    TransferSupervisor.pull(
      ctx.host,
      ctx.bound_port,
      file_path,
      entry,
      ctx.client_sync_dir,
      ctx.client_index_name
    )
  end

  # -----
  # Happy path tests
  # -----
  test "pull file from remote peer -> file written to local sync dir + index updated",
       %{
         client_index_name: client_index_name,
         client_sync_dir: client_sync_dir,
         test_entry: test_entry,
         test_relative_path: test_relative_path,
         test_file_checksum: test_file_checksum
       } = ctx do
    assert Index.get(client_index_name, test_relative_path) == nil
    refute File.exists?(Path.join(client_sync_dir, test_relative_path))

    pull_file(ctx, test_relative_path, test_entry)

    %Entry{checksum: post_pull_checksum} = Index.get(client_index_name, test_relative_path)
    # Ensure generated checksum is the same as the checksum computed in setup
    assert post_pull_checksum == test_file_checksum
    # Ensure file contents match
    assert File.read!(Path.join(client_sync_dir, test_relative_path)) == "asdf"
    # Ensure that the file exists on local sync dir
    assert File.exists?(Path.join(client_sync_dir, test_relative_path))
    # Ensure .tmp was rm'ed
    refute File.exists?(Path.join(client_sync_dir, test_relative_path <> ".tmp"))
  end

  test "checksum mismatch -> no index entry, nor local file",
       %{
         client_index_name: client_index_name,
         client_sync_dir: client_sync_dir,
         test_entry: test_entry,
         test_relative_path: test_relative_path
       } = ctx do
    assert Index.get(client_index_name, test_relative_path) == nil
    refute File.exists?(Path.join(client_sync_dir, test_relative_path))
    %Entry{} = test_entry = %{test_entry | checksum: "bogus_checksum"}

    pull_file(ctx, test_relative_path, test_entry)

    # No index, no file, no tmp file
    assert Index.get(client_index_name, test_relative_path) == nil
    refute File.exists?(Path.join(client_sync_dir, test_relative_path))
    refute File.exists?(Path.join(client_sync_dir, test_relative_path <> ".tmp"))
  end

  test "sweep_orphans -> remove all untracked .tmp files", %{
    client_sync_dir: client_sync_dir,
    client_index_name: client_index_name
  } do
    # Create two files, and add one to the client index
    in_index_file_name = "file0.tmp"
    not_in_index_file_name = "file1.tmp"
    in_index_file_path = Path.join(client_sync_dir, in_index_file_name)
    not_in_index_file_path = Path.join(client_sync_dir, not_in_index_file_name)
    Index.local_write(client_index_name, in_index_file_name, "asdf", 1)
    File.write!(in_index_file_path, "Mewtwo")
    File.write!(not_in_index_file_path, "Moltres")
    # Call sweep_orphans directly which should remove any files ending with .tmp
    # not in the index.
    DataPlane.sweep_orphans(client_sync_dir, client_index_name)
    assert File.exists?(in_index_file_path)
    refute File.exists?(not_in_index_file_path)
  end

  # -----
  # Error path tests
  # -----
  test "path traversal is prevented by validating sync dir",
       %{
         client_index_name: client_index_name,
         client_sync_dir: client_sync_dir,
         test_entry: test_entry
       } = ctx do
    # TODO: This test does not actually exercise the path traversal guard. It would
    # if the ../../../etc/passwd path referred to a real file. Right now this just
    # tests that the error is returned for a non-existent file.
    test_file_path = "#{client_sync_dir}/../../../etc/passwd"
    assert Index.get(client_index_name, test_file_path) == nil
    refute File.exists?(Path.join(client_sync_dir, test_file_path))

    {:error, :enoent} = pull_file(ctx, "../../../etc/passwd", test_entry)

    # No index, no file, no tmp file
    assert Index.get(client_index_name, test_file_path) == nil
    refute File.exists?(Path.join(client_sync_dir, test_file_path))
    refute File.exists?(Path.join(client_sync_dir, test_file_path <> ".tmp"))
  end
end
