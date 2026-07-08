defmodule SyncSet.ControlTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir
  alias SyncSet.{Control, Entry, Index, VersionVector}

  # Port the announcing peer advertises; Control threads it into the transfer.
  @peer_port 4040

  setup %{tmp_dir: tmp_dir} do
    uniq = System.unique_integer([:positive])
    # sync_dir and DETS file live under ExUnit's tmp_dir, wiped before each test
    sync_dir = Path.join(tmp_dir, "sync_#{uniq}")
    :ok = File.mkdir_p!(sync_dir)
    path = Path.join(tmp_dir, "idx_#{uniq}.dets")
    table = String.to_atom("table_#{uniq}")
    idx_name = String.to_atom("idx_name_#{uniq}")
    ctrl_name = String.to_atom("ctrl_name_#{uniq}")

    test_pid = self()
    # transfer_fn seam: forward what Control decided to fetch into the test mailbox
    # instead of opening a real TCP pull, so tests can assert on it.
    transfer_fn = fn peer, key, entry -> send(test_pid, {:transfer, peer, key, entry}) end

    idx_opts = [dets_path: path, table: table, name: idx_name]

    ctrl_opts = [
      index: idx_name,
      data_plane: self(),
      sync_dir: sync_dir,
      transfer_fn: transfer_fn,
      name: ctrl_name
    ]

    start_supervised!({Index, idx_opts})
    start_supervised!({Control, ctrl_opts})
    {:ok, idx_name: idx_name, ctrl_name: ctrl_name, sync_dir: sync_dir}
  end

  test "an announce we already dominate -> no transfer requested", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name
  } do
    # Index is keyed by sync_dir-relative paths
    key = "file.txt"
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, key, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, key)

    # Incoming announce carries an older (empty) vector -> current :dominates it
    incoming = %Entry{vector: %{}, checksum: "incoming_checksum", size: 5, deleted: false}
    GenServer.cast(ctrl_name, {:announce, key, incoming, :peer@nohost, @peer_port})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)

    # No transfer requested, index entry untouched
    refute_receive {:transfer, _, _, _}
    assert %Entry{vector: ^current_vector} = Index.get(idx_name, key)

    # Now equal case:
    incoming = %Entry{
      vector: current_vector,
      checksum: "current_checksum",
      size: 10,
      deleted: false
    }

    GenServer.cast(ctrl_name, {:announce, key, incoming, :peer@nohost, @peer_port})
    :sys.get_state(ctrl_name)
    refute_receive {:transfer, _, _, _}
    assert %Entry{vector: ^current_vector} = Index.get(idx_name, key)
  end

  test "an announce that dominates us -> transfer requested", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name
  } do
    from_node = :peer@nohost
    key = "file.txt"
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, key, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, key)
    incoming_vector = VersionVector.increment(current_vector, from_node)
    # Incoming announce carries a newer vector -> current is :dominated by it
    incoming = %Entry{
      vector: incoming_vector,
      checksum: "incoming_checksum",
      size: 5,
      deleted: false
    }

    GenServer.cast(ctrl_name, {:announce, key, incoming, from_node, @peer_port})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)

    # Transfer requested against the announcing peer's host+port; index untouched
    assert_receive {:transfer, %{host: ~c"nohost", port: @peer_port}, ^key,
                    %Entry{checksum: "incoming_checksum"}},
                   5000

    assert %Entry{vector: ^current_vector} = Index.get(idx_name, key)
  end

  test "a dominating tombstone announce -> apply_remote + local file deleted", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name,
    sync_dir: sync_dir
  } do
    from_node = :peer@nohost
    key = "file.txt"
    file_path = Path.join(sync_dir, key)
    File.write!(file_path, "some bytes")
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, key, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, key)
    incoming_vector = VersionVector.increment(current_vector, from_node)
    # Incoming announce carries a newer vector -> current is :dominated by it
    incoming = %Entry{
      vector: incoming_vector,
      checksum: nil,
      size: 0,
      deleted: true
    }

    GenServer.cast(ctrl_name, {:announce, key, incoming, from_node, @peer_port})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)
    # File deleted at its sync_dir-relative location
    refute File.exists?(file_path)

    # No transfer requested, index entry updated, file deleted
    refute_receive {:transfer, _, _, _}
    assert %Entry{deleted: true, checksum: nil} = Index.get(idx_name, key)
  end

  test "concurrent tombstone -> file survives", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name,
    sync_dir: sync_dir
  } do
    from_node = :peer@nohost
    other_node = :other_peer@nohost
    key = "file.txt"
    file_path = Path.join(sync_dir, key)
    File.write!(file_path, "some bytes")
    # Local write bumps our vector to %{<node> => 1}
    Index.local_write(idx_name, key, "current_checksum", 10)
    %Entry{vector: current_vector} = Index.get(idx_name, key)
    incoming_vector = %{other_node => 2}
    # Incoming announce carries a concurrent vector
    assert VersionVector.compare(current_vector, incoming_vector) == :concurrent

    incoming = %Entry{
      vector: incoming_vector,
      checksum: nil,
      size: 0,
      deleted: true
    }

    GenServer.cast(ctrl_name, {:announce, key, incoming, from_node, @peer_port})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)
    # File survives the concurrent delete
    assert File.exists?(file_path)

    # No transfer requested, index entry not tombstoned, file not deleted
    refute_receive {:transfer, _, _, _}
    assert %Entry{deleted: false} = Index.get(idx_name, key)
  end

  test "absent entry -> transfer", %{
    ctrl_name: ctrl_name,
    idx_name: idx_name
  } do
    from_node = :peer@nohost
    key = "file.txt"
    other_node = :other_peer@nohost
    incoming_vector = %{other_node => 1}

    incoming = %Entry{
      vector: incoming_vector,
      checksum: "incoming_checksum",
      size: 5,
      deleted: false
    }

    GenServer.cast(ctrl_name, {:announce, key, incoming, from_node, @peer_port})

    # Block until Control has processed the message
    :sys.get_state(ctrl_name)

    # Transfer requested; index entry left untouched (bytes land + apply after verify)
    assert_receive {:transfer, %{host: ~c"nohost", port: @peer_port}, ^key,
                    %Entry{checksum: "incoming_checksum"}},
                   5000

    # Control only requested bytes; it must NOT have written metadata
    assert Index.get(idx_name, key) == nil
  end
end
