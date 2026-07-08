defmodule SyncSet.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  alias SyncSet.{Config, Membership}

  @impl true
  def start(_type, _args) do
    # TransferSupervisor is infra with no sync_dir dependency, so it always starts.
    # The node's runtime processes are gated (see runtime_children/0): in :test we
    # skip them so each test can start only the isolated subtree it needs.
    children = [{Task.Supervisor, name: SyncSet.TransferSupervisor}] ++ runtime_children()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SyncSet.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # The live node: watch/index/replicate. Gated behind :start_runtime? (default true)
  # so :test can disable it. Config.sync_dir/0 is only read inside the guard, so the
  # suite never triggers its "SYNC_DIR required" raise.
  defp runtime_children do
    if Application.get_env(:sync_set, :start_runtime?, true) do
      sync_dir = Config.sync_dir()
      File.mkdir_p!(sync_dir)
      dets_path = Config.dets_path()
      data_port = Config.data_port()
      topologies = [sync_set: Membership.topology_from_env(System.get_env())]

      [
        # Index: all other GenServers depend on it
        {SyncSet.Index, dets_path: dets_path, name: SyncSet.Index},
        # DataPlane: listener must exist before peers reach us
        {SyncSet.DataPlane,
         port: data_port, sync_dir: sync_dir, index: SyncSet.Index, name: SyncSet.DataPlane},
        # Control: announce/apply hub
        {SyncSet.Control,
         index: SyncSet.Index, data_plane: SyncSet.DataPlane, name: SyncSet.Control},
        # Scanner: kick off cold start scan early
        {SyncSet.Scanner, sync_dir: sync_dir, index: SyncSet.Index, control: SyncSet.Control},
        # LocalChange: subscriber for Watcher event
        {SyncSet.LocalChange,
         index: SyncSet.Index, control: SyncSet.Control, name: SyncSet.LocalChange},
        # Watcher: start watching ongoing changes last among local pieces
        {SyncSet.Watcher, sync_dir: sync_dir, subscriber: SyncSet.LocalChange},
        # Cluster Supervisor: join cluster only once local state is ready
        {Cluster.Supervisor, [topologies, [name: SyncSet.ClusterSupervisor]]},
        # Reconciler: last -- reacts to peers
        {SyncSet.Reconciler,
         index: SyncSet.Index, sync_dir: sync_dir, data_plane: SyncSet.DataPlane}
      ]
    else
      []
    end
  end
end
