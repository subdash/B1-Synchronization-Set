defmodule SyncSet.Index do
  alias SyncSet.Replica
  use GenServer

  # --- Client functions ---
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def get(server, path) do
    GenServer.call(server, {:get, path})
  end

  def snapshot(server) do
    GenServer.call(server, :snapshot)
  end

  def local_write(server, path, checksum, size) do
    GenServer.call(server, {:local_write, path, checksum, size})
  end

  def local_delete(server, path) do
    GenServer.call(server, {:local_delete, path})
  end

  def apply_remote(server, path, entry) do
    GenServer.call(server, {:apply_remote, path, entry})
  end

  # --- Server callbacks ---
  @impl true
  def init(opts) do
    # This guarantees terminate/2 will run and close DETS cleanly on shutdown
    Process.flag(:trap_exit, true)
    # Read the configuration options
    node = Keyword.get(opts, :node, Node.self())
    # dets_path is REQUIRED
    dets_path = Keyword.get(opts, :dets_path)
    table = Keyword.get(opts, :table, :sync_set_index)
    # Open the table
    {:ok, ^table} = :dets.open_file(table, file: to_charlist(dets_path), type: :set)
    # Build starting replica by folding the on-disk rows into a fresh replica
    replica =
      :dets.foldl(
        fn {path, entry}, acc ->
          Replica.apply_remote(acc, path, entry)
        end,
        Replica.new(node),
        table
      )

    {:ok, %{replica: replica, table: table}}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
  end

  @impl true
  def handle_call({:get, path}, _from, state) do
    {:reply, Replica.get(state.replica, path), state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, state.replica.index, state}
  end

  @impl true
  def handle_call({:local_write, path, checksum, size}, _from, state) do
    # Save current index state and write to the replica
    before = state.replica.index
    {replica, _entry} = Replica.local_write(state.replica, path, checksum, size)
    # Persist changes to det table and return updated replica
    persist_changes(before, replica.index, state.table)
    {:reply, :ok, %{state | replica: replica}}
  end

  @impl true
  def handle_call({:local_delete, path}, _from, state) do
    before = state.replica.index
    {replica, _entry} = Replica.local_delete(state.replica, path)
    persist_changes(before, replica.index, state.table)
    {:reply, :ok, %{state | replica: replica}}
  end

  @impl true
  def handle_call({:apply_remote, path, incoming_entry}, _from, state) do
    before = state.replica.index
    replica = Replica.apply_remote(state.replica, path, incoming_entry)
    persist_changes(before, replica.index, state.table)
    {:reply, :ok, %{state | replica: replica}}
  end

  # --- Private functions ---
  defp persist_changes(before_index, after_index, table) do
    keys = Enum.uniq(Map.keys(before_index) ++ Map.keys(after_index))

    # Use a filter expression to only process changes. If the entries in both indexes
    # at the same path contain the same value, skip.
    for path <- keys, Map.get(before_index, path) != Map.get(after_index, path) do
      # Persist the %{path => entry} to dets table
      :dets.insert(table, {path, Map.get(after_index, path)})
    end

    # Flush to disk so a crash right after the call doesn't lose the write
    :dets.sync(table)
  end
end
