defmodule SyncSet.Replica do
  alias SyncSet.{Conflict, VersionVector, Entry, Replica}
  # index is a map of path => Entry
  defstruct node_id: nil, index: %{}

  def new(node) do
    %Replica{node_id: node, index: %{}}
  end

  def get(%Replica{index: index}, path) do
    Map.get(index, path)
  end

  def local_write(replica = %Replica{}, path, checksum, size) do
    # Obtain the corresponding entry's vector or create a new one, bump version for
    # replica node, and return updated replica and entry.
    vector = safe_get_vector(replica, path)
    vector = VersionVector.increment(vector, replica.node_id)
    entry = %Entry{size: size, checksum: checksum, vector: vector, deleted: false}
    replica = put_entry(replica, path, entry)
    {replica, entry}
  end

  def local_delete(replica = %Replica{}, path) do
    case Map.get(replica.index, path) do
      # If the entry is absent or already deleted, make this a no-op, otherwise
      # we could end up with an infinite loop of peers propagating the newer
      # tombstone to each other. Return nil even in the case where the entry exists
      # but is deleted so that we're not announcing a new entry when there is none.
      nil ->
        {replica, nil}

      %Entry{deleted: true} ->
        {replica, nil}

      # If the entry is not deleted, give it a tombstone and bump its vector version 
      # and clear content metadata (checksum/size) so concurrent tombstones converge.
      %Entry{deleted: false} ->
        vector = safe_get_vector(replica, path)
        vector = VersionVector.increment(vector, replica.node_id)

        entry = %Entry{
          size: 0,
          checksum: nil,
          vector: vector,
          deleted: true
        }

        replica = put_entry(replica, path, entry)
        {replica, entry}
    end
  end

  def apply_remote(%Replica{} = replica, path, incoming_entry) do
    # Take an incoming entry and reconcile it with the replica's current index.
    case current = Map.get(replica.index, path) do
      nil ->
        # In the case that there is no entry at the path, write the incoming entry.
        put_entry(replica, path, incoming_entry)

      _ ->
        # No-op when incoming is equal to or dominated by the current -- we already
        # hold a newer-or-equal version.
        case VersionVector.compare(incoming_entry.vector, current.vector) do
          :equal ->
            replica

          :dominated ->
            replica

          :dominates ->
            put_entry(replica, path, incoming_entry)

          :concurrent ->
            resolve(replica, path, current, incoming_entry)
        end
    end
  end

  def merge(into = %Replica{}, other = %Replica{}) do
    # Anti-entropy at the replica level. It brings `into` up to date with everything
    # other_replica knows by replaying each of other's entries using apply_remote.
    # The accumulator is the replica being built up. By iterating over other.index,
    # each {path, entry} is treated as if it had arrived as a single gossip message.
    #
    # Merge maintains the properties of being idempotent (merging twice changes nothing the
    # second time), associative (the final state doesn't depend on the order you merge
    # peers in), and commutative (also doesn't depend on the order in which you iterate over
    # other.index).
    Enum.reduce(other.index, into, fn {path, entry}, acc ->
      apply_remote(acc, path, entry)
    end)
  end

  defp safe_get_vector(replica, path) do
    case Map.get(replica.index, path) do
      nil -> VersionVector.new()
      entry -> entry.vector
    end
  end

  defp resolve(replica, path, current = %Entry{}, incoming = %Entry{}) do
    # Big picture: when there are concurrent updates, we must end up with a state where
    # both replicas' indexes converge. 

    # First, do a simple merge of the maps where the vector with the higher component
    # value always wins.
    merged = VersionVector.merge(current.vector, incoming.vector)

    cond do
      # If there is already a tombstone on both entries, we will return the replica
      # with (current v merged).
      current.deleted && incoming.deleted ->
        put_entry(replica, path, %Entry{current | vector: merged})

      # In either case where only one entry is tombstoned, favor the one that is not. This way
      # a file with conflicts gets resurrected on the node where it was deleted, rather than
      # being deleted/lost on the node where it was present.
      current.deleted ->
        put_entry(replica, path, %Entry{incoming | vector: merged})

      incoming.deleted ->
        put_entry(replica, path, %Entry{current | vector: merged})

      # If neither is deleted but they have the same content, then merge the vectors and
      # return the replica.
      current.checksum == incoming.checksum ->
        put_entry(replica, path, %Entry{current | vector: merged})

      true ->
        # Neither deleted -- this means there are concurrent updates, neither of which
        # contain a tombstoned entry. In this case, we just pick the checksum that is
        # lexicographically greater and return the replica where the corresponding entry
        # wins. We preserve the loser under a sync-conflict path so that no edit is lost.
        {win_entry, lose_entry} =
          case Conflict.winner(current, incoming) do
            :left -> {current, incoming}
            :right -> {incoming, current}
          end

        replica = put_entry(replica, path, %Entry{win_entry | vector: merged})
        conflict_path = Conflict.conflict_path(path, lose_entry.checksum)
        apply_remote(replica, conflict_path, lose_entry)
    end
  end

  defp put_entry(replica = %Replica{}, path, entry) do
    # Helper function for a common operation to update an entry in a replica's index
    index = Map.put(replica.index, path, entry)
    %Replica{replica | index: index}
  end
end
