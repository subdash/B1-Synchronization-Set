defmodule SyncSet.LocalChangeTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  alias SyncSet.{Checksum, Entry, Index, LocalChange}

  setup %{tmp_dir: tmp_dir} do
    # Generate unique names for each test run
    uniq = System.unique_integer([:positive])
    # sync_dir and DETS file live under ExUnit's tmp_dir, wiped before each test
    sync_dir = Path.join(tmp_dir, "sync_#{uniq}")
    :ok = File.mkdir_p!(sync_dir)
    path = Path.join(tmp_dir, "idx_#{uniq}.dets")
    table = String.to_atom("table_#{uniq}")
    idx_name = String.to_atom("idx_name_#{uniq}")
    lc_name = String.to_atom("lc_name_#{uniq}")
    idx_opts = [dets_path: path, table: table, name: idx_name]
    # By passing self() for control, the test process's mailbox receives messages
    # from the LocalChange GenServer
    lc_opts = [index: idx_name, control: self(), name: lc_name]

    start_supervised!({Index, idx_opts})
    start_supervised!({LocalChange, lc_opts})
    {:ok, idx_name: idx_name, lc_name: lc_name, sync_dir: sync_dir}
  end

  test "new content -> write + announce", %{
    lc_name: lc_name,
    sync_dir: sync_dir,
    idx_name: idx_name
  } do
    file_path = Path.join(sync_dir, "file.txt")
    # Write a file to the sync directory
    :ok = File.write!(file_path, "some bytes lol")
    # Hand LocalChange the event that in production would be sent by the Watcher
    send(lc_name, {:stable, file_path})
    assert_receive {:local_change, ^file_path}, 2000
    %Entry{checksum: actual_checksum} = Index.get(idx_name, file_path)
    {:ok, expected_checksum} = Checksum.of_file(file_path)
    assert actual_checksum == expected_checksum
  end

  test "echo dropped", %{lc_name: lc_name, sync_dir: sync_dir, idx_name: idx_name} do
    # Create local entry in sync dir
    file_path = Path.join(sync_dir, "file.txt")
    :ok = File.write!(file_path, "some bytes lol")
    contents = File.read!(file_path)
    checksum = Checksum.of_binary(contents)
    size = byte_size(contents)
    # Write the file to the index
    Index.local_write(idx_name, file_path, checksum, size)
    # Retrieve current vector fore file path
    %Entry{vector: old_vector} = Index.get(idx_name, file_path)
    send(lc_name, {:stable, file_path})
    # Block until :stable handler is done
    :sys.get_state(lc_name)
    # Prove that LocalChange stayed silent since the file did not change
    refute_receive {:local_change, _}
    %Entry{vector: new_vector} = Index.get(idx_name, file_path)
    assert old_vector == new_vector
  end

  test "delete live file -> tombstone + announce", %{
    lc_name: lc_name,
    sync_dir: sync_dir,
    idx_name: idx_name
  } do
    # Fabricate a file path in the sync dir
    file_path = Path.join(sync_dir, "file.txt")
    # Create an entry in the index
    Index.local_write(idx_name, file_path, "abcd1234", 1)
    # Assert entry is not yet deleted
    %Entry{deleted: deleted_before} = Index.get(idx_name, file_path)
    assert !deleted_before
    # Trigger deletion and sync state
    send(lc_name, {:deleted, file_path})
    :sys.get_state(lc_name)
    # Validate that a change was announced for the file and that the file
    # was tombstoned in its index.
    assert_receive {:local_change, ^file_path}, 2000
    %Entry{deleted: deleted_after} = Index.get(idx_name, file_path)
    assert deleted_after
  end

  test "delete absent file -> nothing", %{
    lc_name: lc_name,
    sync_dir: sync_dir,
    idx_name: idx_name
  } do
    # Fabricate a file path in the sync dir
    file_path = Path.join(sync_dir, "file.txt")
    send(lc_name, {:deleted, file_path})
    :sys.get_state(lc_name)
    # Validate that no entry was created, and no announce
    assert nil == Index.get(idx_name, file_path)
    refute_receive {:local_change, ^file_path}
  end

  test "delete already-deleted file -> nothing", %{
    lc_name: lc_name,
    sync_dir: sync_dir,
    idx_name: idx_name
  } do
    # Fabricate a file path in the sync dir
    file_path = Path.join(sync_dir, "file.txt")
    # Create an entry in the index
    Index.local_write(idx_name, file_path, "abcd1234", 1)
    # First delete
    send(lc_name, {:deleted, file_path})
    # Assert deletion announced
    assert_receive {:local_change, ^file_path}, 2000
    # Capture vector before
    %Entry{vector: before_vector} = Index.get(idx_name, file_path)
    # Second delete
    send(lc_name, {:deleted, file_path})
    # Sync state and assert no second deletion announced
    :sys.get_state(lc_name)
    %Entry{vector: after_vector} = Index.get(idx_name, file_path)
    assert before_vector == after_vector
    refute_receive {:local_change, _}
  end
end
