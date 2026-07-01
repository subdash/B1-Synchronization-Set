defmodule SyncSet.ReconcilerIntegrationTest do
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
    test_pid = self()

    exchange_fn = fn node ->
      send(test_pid, {:exchanged, node})
      {%{}, 0}
    end

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
      name: reconciler_name,
      exchange_fn: exchange_fn
    ]

    # DataPlane and Reconciler reference Index by name, and Reconciler
    # references DataPlane by name, hence the ordering.
    start_supervised!({Index, index_opts})
    start_supervised!({DataPlane, data_plane_opts})
    start_supervised!({Reconciler, reconciler_opts})

    %{
      index: index_name,
      data_plane: data_plane_name,
      reconciler: reconciler_name
    }
  end

  test "{:nodedown, _} -> no-op", %{
    reconciler: reconciler
  } do
    before_state = :sys.get_state(reconciler)
    # Async nodedown message
    send(reconciler, {:nodedown, :fake@host})
    # Sync state
    after_state = :sys.get_state(reconciler)
    assert before_state == after_state
  end

  test "{:nodeup, _} -> fresh exchange", %{
    reconciler: reconciler
  } do
    send(reconciler, {:nodeup, :fake@host})
    assert_receive {:exchanged, :fake@host}

    # now the plan's reconnect: down, then up again
    send(reconciler, {:nodedown, :fake@host})
    send(reconciler, {:nodeup, :fake@host})
    # a FRESH exchange round
    assert_receive {:exchanged, :fake@host}
  end
end
