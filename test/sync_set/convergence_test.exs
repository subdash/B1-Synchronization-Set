defmodule SyncSet.ConvergenceTest do
  use ExUnit.Case
  use ExUnitProperties
  alias SyncSet.{Replica, Checksum}
  # alias SyncSet.Checksum

  property "gossiping replicas converge" do
    check all(ops <- list_of(operation(), max_length: 20)) do
      nodes = [:a, :b, :c]
      replicas = Map.new(nodes, fn n -> {n, Replica.new(n)} end)
      replicas = Enum.reduce(ops, replicas, &apply_op/2)
      converged = gossip_to_fixpoint(replicas)
      indexes = converged |> Map.values() |> Enum.map(& &1.index) |> Enum.uniq()
      assert length(indexes) == 1
    end
  end

  defp operation() do
    tuple({
      member_of([:a, :b, :c]),
      member_of(["f1.txt", "f2.txt", "f3.txt"]),
      one_of([
        tuple({constant(:write), member_of(["alpha", "beta", "gamma"])}),
        constant(:delete)
      ])
    })
  end

  defp apply_op({origin, path, kind}, replicas_map) do
    r = replicas_map[origin]

    {r2, _entry} =
      case kind do
        {:write, content} ->
          Replica.local_write(r, path, Checksum.of_binary(content), byte_size(content))

        :delete ->
          Replica.local_delete(r, path)
      end

    Map.put(replicas_map, origin, r2)
  end

  defp gossip_to_fixpoint(replicas_map, rounds \\ 10)

  defp gossip_to_fixpoint(replicas_map, 0), do: replicas_map

  defp gossip_to_fixpoint(replicas_map, rounds) do
    peers = Map.values(replicas_map)

    next =
      for {node, r} <- replicas_map, into: %{} do
        {node, Enum.reduce(peers, r, fn peer, acc -> Replica.merge(acc, peer) end)}
      end

    if indexes(next) == indexes(replicas_map) do
      next
    else
      gossip_to_fixpoint(next, rounds - 1)
    end
  end

  defp indexes(replicas_map) do
    Map.new(replicas_map, fn {node, r} -> {node, r.index} end)
  end
end
