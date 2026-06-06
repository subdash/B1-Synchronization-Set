defmodule SyncSet.VersionVector do
  # Take the union of all keys from both vectors, treat a missing key
  # as 0, and compare component-wise across that union:
  # - all components equal → :equal
  # - v1 ≥ v2 everywhere and > somewhere → :dominates
  # - v2 ≥ v1 everywhere and > somewhere → :dominated
  # - otherwise → :concurrent
  def compare(v1, v2) do
    v1_keys = MapSet.new(Map.keys(v1))
    v2_keys = MapSet.new(Map.keys(v2))
    union_of_keys = MapSet.union(v1_keys, v2_keys)

    # Are the vectors identical?
    if Enum.all?(union_of_keys, fn key ->
         Map.get(v1, key, 0) == Map.get(v2, key, 0)
       end) do
      :equal
    else
      covers?(v1, v2)
    end
  end

  def new() do
    %{}
  end

  def increment(vv, node) do
    Map.update(vv, node, 1, &(&1 + 1))
  end

  # Our merge function must be idempotent, commutative and associate. This is
  # important in anti-entropy because a node may receive and re-merge with the
  # same peer state many times.
  def merge(a, b) do
    Map.merge(a, b, fn _key, v1, v2 -> max(v1, v2) end)
  end

  defp covers?(v1, v2) do
    cond do
      Enum.all?(v2, fn {node, count} ->
        Map.get(v1, node, 0) >= count
      end) ->
        :dominates

      Enum.all?(v1, fn {node, count} ->
        Map.get(v2, node, 0) >= count
      end) ->
        :dominated

      true ->
        :concurrent
    end
  end
end
