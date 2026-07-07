defmodule SyncSet.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  alias SyncSet.Membership

  @impl true
  def start(_type, _args) do
    topologies = [sync_set: Membership.topology_from_env(System.get_env())]

    children = [
      # Starts a worker by calling: SyncSet.Worker.start_link(arg)
      # {SyncSet.Worker, arg}
      {Task.Supervisor, name: SyncSet.TransferSupervisor},
      {Cluster.Supervisor, [topologies, [name: SyncSet.ClusterSupervisor]]}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SyncSet.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
