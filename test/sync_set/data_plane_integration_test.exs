defmodule SyncSet.DataPlaneIntegrationTest do
  alias SyncSet.{Checksum, DataPlane, Entry, Index, TransferSupervisor}
  use ExUnit.Case, async: false
  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    uniq = System.unique_integer([:positive])
    # ExUnit's tmp_dir is wiped before each test, so nothing leaks across runs
    # Need separate sync dirs to exercise server vs client
    server_sync_dir = Path.join(tmp_dir, "server_sync_dir")
    client_sync_dir = Path.join(tmp_dir, "client_sync_dir")
    :ok = File.mkdir_p!(server_sync_dir)
    :ok = File.mkdir_p!(client_sync_dir)
    # Point Config.sync_dir/0 at the client dir
    prev = Application.get_env(:sync_set, :sync_dir)
    Application.put_env(:sync_set, :sync_dir, client_sync_dir)

    server_index_name = String.to_atom("server_name_#{uniq}")
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
      name: SyncSet.Index
    ]

    data_plane_opts = [port: 0, sync_dir: server_sync_dir, index: server_index_name]

    start_supervised!({Index, server_idx_opts}, id: :server_index)
    start_supervised!({Index, client_idx_opts}, id: :client_index)
    # start_supervised!({Task.Supervisor, name: TransferSupervisor})
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
    peer = %{host: ~c"127.0.0.1", port: bound_port}

    # Restore previous config on exit (tmp_dir is auto-wiped by ExUnit)
    on_exit(fn ->
      Application.put_env(:sync_set, :sync_dir, prev)
    end)

    {:ok,
     peer: peer,
     client_sync_dir: client_sync_dir,
     test_entry: test_entry,
     test_relative_path: test_relative_path,
     test_file_checksum: test_file_checksum}
  end

  test "start_transfer -> file created which matches peer, tmp file rm'ed, entry created in client index",
       %{
         peer: peer,
         client_sync_dir: client_sync_dir,
         test_entry: test_entry,
         test_relative_path: test_relative_path
       } do
    full_file_path = Path.join(client_sync_dir, test_relative_path)
    assert Index.get(SyncSet.Index, test_relative_path) == nil
    refute File.exists?(full_file_path)

    {:ok, pid} = TransferSupervisor.start_transfer(peer, test_relative_path, test_entry)
    assert is_pid(pid)

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 2_000

    assert File.exists?(full_file_path)
    assert File.read!(full_file_path) == "asdf"
    refute File.exists?(Path.join(client_sync_dir, test_relative_path <> ".tmp"))
    assert %Entry{} = Index.get(SyncSet.Index, test_relative_path)
  end
end
