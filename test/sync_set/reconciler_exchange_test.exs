defmodule SyncSet.ReconcilerExchangeTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir
  @moduletag :capture_log
  alias SyncSet.{DataPlane, Index, Reconciler}

  setup %{tmp_dir: tmp_dir} do
    uniq = System.unique_integer([:positive])
    sync_dir = Path.join(tmp_dir, "sync_dir")
    :ok = File.mkdir_p!(sync_dir)

    index_name = String.to_atom("rx_index_#{uniq}")
    data_plane_name = String.to_atom("rx_data_plane_#{uniq}")
    reconciler_name = String.to_atom("rx_reconciler_#{uniq}")

    index_opts = [
      dets_path: Path.join(tmp_dir, "idx_#{uniq}.dets"),
      table: String.to_atom("rx_table_#{uniq}"),
      name: index_name
    ]

    # port: 0 -> OS-assigned; get_port reports whatever it bound
    data_plane_opts = [
      port: 0,
      sync_dir: sync_dir,
      index: index_name,
      name: data_plane_name
    ]

    reconciler_opts = [
      data_plane: data_plane_name,
      sync_dir: sync_dir,
      index: index_name,
      name: reconciler_name
    ]

    # DataPlane and Reconciler reference Index by name, and Reconciler
    # references DataPlane by name, hence the ordering.
    start_supervised!({Index, index_opts})
    start_supervised!({DataPlane, data_plane_opts})
    start_supervised!({Reconciler, reconciler_opts})

    %{
      index: index_name,
      data_plane: data_plane_name,
      name: reconciler_name
    }
  end

  test ":exchange responder returns snapshot, port", %{
    index: index_name,
    data_plane: data_plane_name,
    name: reconciler_name
  } do
    path0 = "file0"
    path1 = "file1"
    path2 = "file2"
    Index.local_write(index_name, path0, "abc", 4)
    Index.local_write(index_name, path1, "bcd", 5)
    Index.local_write(index_name, path2, "cde", 6)
    expected_snapshot = Index.snapshot(index_name)
    expected_port = DataPlane.get_port(data_plane_name)

    {snapshot, port} = GenServer.call(reconciler_name, :exchange)

    assert expected_snapshot == snapshot
    assert expected_port == port
  end
end
