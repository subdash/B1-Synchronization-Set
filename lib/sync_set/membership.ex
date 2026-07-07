defmodule SyncSet.Membership do
  def topology_from_env(env_map) do
    strategy = Map.get(env_map, "SYNC_CLUSTER_STRATEGY", "gossip")

    case String.downcase(strategy) do
      "epmd" ->
        epmd_hosts =
          env_map
          |> Map.get("SYNC_CLUSTER_HOSTS", "")
          |> String.split(",", trim: true)
          |> Enum.map(&String.trim/1)
          |> Enum.reject(&(&1 == ""))
          |> Enum.map(&String.to_atom/1)

        [strategy: Cluster.Strategy.Epmd, config: [hosts: epmd_hosts]]

      "gossip" ->
        [strategy: Cluster.Strategy.Gossip, config: []]

      _ ->
        raise "unknown SYNC_CLUSTER_STRATEGY: #{inspect(strategy)}"
    end
  end

  def peers, do: Node.list()
end
