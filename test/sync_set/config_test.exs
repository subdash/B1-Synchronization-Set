defmodule SyncSet.ConfigTest do
  use ExUnit.Case
  alias SyncSet.Config

  setup do
    # Restore original vars after test
    original_data_port = Application.get_env(:sync_set, :data_port)
    original_sync_dir = Application.get_env(:sync_set, :sync_dir)
    original_dets_path = Application.get_env(:sync_set, :dets_path)

    on_exit(fn ->
      if original_data_port do
        Application.put_env(:sync_set, :data_port, original_data_port)
      else
        Application.delete_env(:sync_set, :data_port)
      end

      if original_sync_dir do
        Application.put_env(:sync_set, :sync_dir, original_sync_dir)
      else
        Application.delete_env(:sync_set, :sync_dir)
      end

      if original_dets_path do
        Application.put_env(:sync_set, :dets_path, original_dets_path)
      else
        Application.delete_env(:sync_set, :dets_path)
      end
    end)
  end

  test "Config.data_port/0 returns default and override" do
    current_port = Config.data_port()
    assert current_port == 4040
    Application.put_env(:sync_set, :data_port, 5000)
    assert Config.data_port() == 5000
  end

  test "Config.node_id/0 returns the current node" do
    node_id = Node.self()
    assert Config.node_id() == node_id
  end

  test "Config.sync_dir/0 returns system setting, no default" do
    assert Config.sync_dir() == nil
    Application.put_env(:sync_set, :sync_dir, "/some/dir")
    assert Config.sync_dir() == "/some/dir"
  end

  test "Config.dets_path/0 returns default and override" do
    current_dets_path = Config.dets_path()
    assert current_dets_path == "tmp/sync_set.dets"
    Application.put_env(:sync_set, :dets_path, "tmp/wync_wet.dets")
    assert Config.dets_path() == "tmp/wync_wet.dets"
  end
end
