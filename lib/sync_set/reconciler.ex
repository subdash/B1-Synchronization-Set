defmodule SyncSet.Reconciler do
  @moduledoc """
  Anti-entropy between a node and its peers.

  When a peer connects (or reconnects), exchange full index snapshots and converge
  this node's index and disk toward the peer's — pulling content it's missing,
  applying deletes, and preserving conflicting edits. This is what makes correctness
  independent of who was online when a change first happened: a node that was offline
  or missed a broadcast catches up here.
  """
  require Logger
  alias SyncSet.{Checksum, DataPlane, Index, TransferSupervisor}
  use GenServer

  def start_link(opts) do
    {start_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, start_opts)
  end

  @impl true
  def init(opts) do
    :net_kernel.monitor_nodes(true)

    index = Keyword.fetch!(opts, :index)
    sync_dir = Keyword.fetch!(opts, :sync_dir)
    data_plane = Keyword.fetch!(opts, :data_plane)
    exchange_fn = Keyword.get(opts, :exchange_fn, &default_exchange/1)

    state = %{
      index: index,
      sync_dir: sync_dir,
      data_plane: data_plane,
      exchange_fn: exchange_fn
    }

    {:ok, state, {:continue, :reconcile_existing}}
  end

  @impl true
  def handle_info({:nodeup, node}, state) do
    Task.Supervisor.start_child(SyncSet.TransferSupervisor, fn ->
      reconcile_with_peer(node, state)
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, _node}, state) do
    {:noreply, state}
  end

  # So on reconnect, the node will ask each peer in the distribution for its snapshot
  # and attempt to reconcile its local index and disk with that of the peer.
  @impl true
  def handle_continue(:reconcile_existing, state) do
    for node <- Node.list() do
      Task.Supervisor.start_child(SyncSet.TransferSupervisor, fn ->
        reconcile_with_peer(node, state)
      end)
    end

    {:noreply, state}
  end

  # Answer a peer's request to reconcile: hand back our current view of the world —
  # our full index snapshot plus the port to reach our data plane for any bytes the
  # peer decides it needs. This is the "here's what I have" endpoint every node
  # exposes so others can converge toward it.
  @impl true
  def handle_call(:exchange, _from, state) do
    reply = {Index.snapshot(state.index), DataPlane.get_port(state.data_plane)}
    {:reply, reply, state}
  end

  # Destructure the host and port from the peer, then call the reconcile_snapshot
  # driver function to actually reconcile the two nodes.
  #
  # Public for testing
  def reconcile_with_peer(node, state) do
    host =
      Atom.to_string(node)
      |> String.split("@")
      |> List.last()
      |> String.to_charlist()

    try do
      {snapshot, peer_data_plane_port} = state.exchange_fn.(node)
      reconcile_snapshot(snapshot, host, peer_data_plane_port, state)
    catch
      # If the peer is not reachable, it probably hasn't finished booting yet,
      # so we can just log a warning. It will retry on the next connect event.
      :exit, reason ->
        Logger.warning("reconcile with #{node} failed: #{inspect(reason)}")
    end
  end

  # Brings this node into agreement with a peer's view of the folder.
  #
  # Given a peer's full index snapshot, converge our index and our disk toward it:
  # adopt any changes the peer has that we don't, materialize the resulting files
  # (fetch missing content, remove deleted files, preserve the losing side of a
  # conflict), and leave alone anything we already hold an equal-or-newer version of.
  #
  # This is the workhorse of anti-entropy: it makes correctness independent of who
  # was connected when a change first happened. A node that was offline — or just
  # missed a broadcast — catches up here on (re)connect, and because applying a
  # snapshot is idempotent, running it repeatedly is always safe and converges.
  #
  # Public for testing
  def reconcile_snapshot(
        snapshot,
        host,
        port,
        state,
        transfer_fn \\ &TransferSupervisor.start_transfer/3
      ) do
    Enum.each(snapshot, fn {path, entry} ->
      outcome = Index.apply_remote(state.index, path, entry)

      absolute_path = Path.join(state.sync_dir, path)

      case outcome do
        {:conflict, conflict_path, :local} ->
          File.cp(absolute_path, Path.join(state.sync_dir, conflict_path))

        # TODO(convergence): the `:remote` conflict copy — the loser whose bytes
        # we don't hold — is intentionally not materialized here; we expect to
        # pull it from the peer on a *later* exchange. But anti-entropy only runs
        # on nodeup, so with no further connect event this node can retain a
        # conflict-path index entry with no bytes on disk. Needs a periodic
        # anti-entropy tick (or announcing conflict copies) to guarantee it
        # converges. Verify with a two-node test in Task 9.
        _ ->
          :ok
      end

      local = Index.get(state.index, path)

      cond do
        local.deleted ->
          File.rm(absolute_path)

        have_bytes?(absolute_path, local.checksum) ->
          :ok

        true ->
          transfer_fn.(%{host: host, port: port}, path, local)
      end
    end)
  end

  # Ask a peer for its snapshot so we can converge toward it. This is the real way
  # we reach a peer — a call to its Reconciler over Erlang distribution — and the
  # default behind the injectable exchange_fn seam (tests swap in a stub instead of
  # a live node).
  defp default_exchange(node) do
    GenServer.call({SyncSet.Reconciler, node}, :exchange)
  end

  defp have_bytes?(path, checksum) do
    match?({:ok, ^checksum}, Checksum.of_file(path))
  end
end
