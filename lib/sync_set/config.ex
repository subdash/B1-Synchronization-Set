defmodule SyncSet.Config do
  def sync_dir() do
    Application.get_env(:sync_set, :sync_dir)
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

  def topologies() do
    Application.get_env(:sync_set, :topologies, [])
  end
end
