defmodule SyncSet.Control do
  use GenServer
  alias SyncSet.{Entry, Index, VersionVector}

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    data_plane = Keyword.fetch!(opts, :data_plane)
    state = %{index: index, data_plane: data_plane}
    {:ok, state}
  end

  def start_link(opts) do
    {start_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, start_opts)
  end

  @impl true
  def handle_info({:local_change, path}, state) do
    # The node's LocalChange process updated its index and announced a change to an entry.
    # This will crash if there is no entry on the index, which should never happen if
    # LocalChange is correctly/consistently writing to the index before sending the local_change
    # message.
    %Entry{} = entry = Index.get(state.index, path)
    # Inter-node message to announce to peers that a path's entry has changed
    GenServer.abcast(
      Node.list(),
      SyncSet.Control,
      {:announce, path, entry, Node.self()}
    )

    {:noreply, state}
  end

  @impl true
  def handle_info({:want, path, reply_to}, state) do
    send(state.data_plane, {:serve, reply_to, path})
    {:noreply, state}
  end

  @impl true
  def handle_info({:announce, path, incoming_entry, from_node}, state) do
    # Another node broadcast an announce message with its own local entry at path.
    # Compare version vectors and decide what to do next.
    case Index.get(state.index, path) do
      nil ->
        # No local entry -> incoming is new -> pull or apply tombstone 
        act(path, nil, incoming_entry, from_node, state)

      current ->
        case VersionVector.compare(current.vector, incoming_entry.vector) do
          # Current dominates or is equal -> no-op
          result when result in [:equal, :dominates] ->
            {:noreply, state}

          # Delegate to act to handle tombstoned entry or dominating incoming entry
          result when result in [:dominated, :concurrent] ->
            act(path, current, incoming_entry, from_node, state)
        end
    end
  end

  # How to handle new/incoming entry
  defp act(path, current, incoming, from_node, state) do
    cond do
      # Tombstone. No fetch. Update index and delete local file if current/incoming agree.
      incoming.checksum == nil ->
        Index.apply_remote(state.index, path, incoming)

        case Index.get(state.index, path) do
          # Only delete the file if the local index agrees that it is deleted
          %Entry{deleted: true} ->
            File.rm(path)

          _ ->
            :ok
        end

        {:noreply, state}

      # We already have these bytes. Do nothing.
      current != nil and incoming.checksum == current.checksum ->
        {:noreply, state}

      # Live content we lack -> fetch it
      true ->
        send(state.data_plane, {:pull, from_node, path, incoming.checksum})
        {:noreply, state}
    end
  end
end
