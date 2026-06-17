defmodule SyncSet.ControlTest do
  use ExUnit.Case, async: true
  alias SyncSet.{Control, Entry, Index, VersionVector}

  setup do
    uniq = System.unique_integer([:positive])
    tmp_dir = System.tmp_dir!()
    sync_dir = Path.join(tmp_dir, "sync_#{uniq}")
    :ok = File.mkdir_p!(sync_dir)
    path = Path.join(tmp_dir, "idx_#{uniq}.dets")
    table = String.to_atom("table_#{uniq}")
    idx_name = String.to_atom("idx_name_#{uniq}")
    ctrl_name = String.to_atom("ctrl_name_#{uniq}")

    idx_opts = [dets_path: path, table: table, name: idx_name]
    # self() as data_plane: pull requests land in test process mailbox
    ctrl_opts = [index: idx_name, data_plane: self(), name: ctrl_name]

    on_exit(fn ->
      File.rm(path)
      File.rm_rf(sync_dir)
    end)

    start_supervised!({Index, idx_opts})
    start_supervised!({Control, ctrl_opts})
    {:ok, idx_name: idx_name, ctrl_name: ctrl_name, sync_dir: sync_dir}
  end

  test "an announce we already dominate -> no pull requested", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name,
    sync_dir: sync_dir
  } do
    file_path = Path.join(sync_dir, "file.txt")
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, file_path, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, file_path)

    # Incoming announce carries an older (empty) vector -> current :dominates it
    incoming = %Entry{vector: %{}, checksum: "incoming_checksum", size: 5, deleted: false}
    send(ctrl_name, {:announce, file_path, incoming, :peer@nohost})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)

    # No pull requested, index entry untouched
    refute_receive {:pull, _, _, _}
    assert %Entry{vector: ^current_vector} = Index.get(idx_name, file_path)

    # Now equal case:
    incoming = %Entry{
      vector: current_vector,
      checksum: "current_checksum",
      size: 10,
      deleted: false
    }

    send(ctrl_name, {:announce, file_path, incoming, :peer@nohost})
    :sys.get_state(ctrl_name)
    refute_receive {:pull, _, _, _}
    assert %Entry{vector: ^current_vector} = Index.get(idx_name, file_path)
  end

  test "an announce that dominates us -> pull requested", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name,
    sync_dir: sync_dir
  } do
    from_node = :peer@nohost
    file_path = Path.join(sync_dir, "file.txt")
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, file_path, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, file_path)
    incoming_vector = VersionVector.increment(current_vector, from_node)
    # Incoming announce carries a newer vector -> current is :dominated by it
    incoming = %Entry{
      vector: incoming_vector,
      checksum: "incoming_checksum",
      size: 5,
      deleted: false
    }

    send(ctrl_name, {:announce, file_path, incoming, :peer@nohost})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)

    # Pull requested, index entry not updated
    assert_receive {:pull, ^from_node, ^file_path, "incoming_checksum"}, 5000
    assert %Entry{vector: ^current_vector} = Index.get(idx_name, file_path)
  end

  test "a dominating tombstone announce -> apply_remote + local file deleted requested", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name,
    sync_dir: sync_dir
  } do
    from_node = :peer@nohost
    file_path = Path.join(sync_dir, "file.txt")
    File.write!(file_path, "some bytes")
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, file_path, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, file_path)
    incoming_vector = VersionVector.increment(current_vector, from_node)
    # Incoming announce carries a newer vector -> current is :dominated by it
    incoming = %Entry{
      vector: incoming_vector,
      checksum: nil,
      size: 0,
      deleted: true
    }

    send(ctrl_name, {:announce, file_path, incoming, :peer@nohost})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)
    # File deleted
    refute File.exists?(file_path)

    # No pull requested, index entry updated, file deleted 
    refute_receive {:pull, _, _, _}
    assert %Entry{deleted: true, checksum: nil} = Index.get(idx_name, file_path)
  end

  test "concurrent tombstone -> file survives", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name,
    sync_dir: sync_dir
  } do
    from_node = :peer@nohost
    other_node = :other_peer@nohost
    file_path = Path.join(sync_dir, "file.txt")
    File.write!(file_path, "some bytes")
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, file_path, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, file_path)
    incoming_vector = %{other_node => 2}
    # Incoming announce carries a concurrent vector
    assert VersionVector.compare(current_vector, incoming_vector) == :concurrent

    incoming = %Entry{
      vector: incoming_vector,
      checksum: nil,
      size: 0,
      deleted: true
    }

    send(ctrl_name, {:announce, file_path, incoming, from_node})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)
    # File deleted
    assert File.exists?(file_path)

    # No pull requested, index entry updated, file not deleted 
    refute_receive {:pull, _, _, _}
    assert %Entry{deleted: false} = Index.get(idx_name, file_path)
  end

  test "absent entry -> pull", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name,
    sync_dir: sync_dir
  } do
    from_node = :peer@nohost
    file_path = Path.join(sync_dir, "file.txt")
    other_node = :other_peer@nohost
    incoming_vector = %{other_node => 1}

    incoming = %Entry{
      vector: incoming_vector,
      checksum: "incoming_checksum",
      size: 5,
      deleted: false
    }

    send(ctrl_name, {:announce, file_path, incoming, from_node})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)

    # Pull requested, index entry left untouched (DataPlane applies after verify)
    assert_receive {:pull, ^from_node, ^file_path, "incoming_checksum"}, 5000
    # Control only requested bytes; it must NOT have written metadata
    assert Index.get(idx_name, file_path) == nil
  end
end
