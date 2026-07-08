defmodule SyncSet.Config do
  def sync_dir() do
    case Application.get_env(:sync_set, :sync_dir) do
      nil -> raise "sync_dir is not configured (set SYNC_DIR, or config :sync_set, :sync_dir)"
      sync_dir -> sync_dir
    end
  end

  def dets_path() do
    Application.get_env(:sync_set, :dets_path, "tmp/sync_set.dets")
  end

  def data_port() do
    Application.get_env(:sync_set, :data_port, 4040)
  end

  def node_id() do
    Node.self()
  end
end
