defmodule SyncSet.Watcher do
  use GenServer
  require Logger

  @impl true
  def init(opts) do
    # Default to FileSystem implementation
    backend = Keyword.get(opts, :backend, SyncSet.Watcher.Backend.FileSystem)
    debounce_ms = Keyword.get(opts, :debounce_ms, 500)
    # sync_dir and subscriber are required -- crash without them
    sync_dir = Keyword.fetch!(opts, :sync_dir)
    subscriber = Keyword.fetch!(opts, :subscriber)

    {:ok, worker_pid} = backend.watch([sync_dir])

    state = %{
      worker_pid: worker_pid,
      subscriber: subscriber,
      debounce_ms: debounce_ms,
      # Map of path => %{timer: ref, size: int, mtime: ...}
      paths: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_info({:file_event, _worker_pid, {path, _events}}, state) do
    # Reschedule a debounce timer for a path, cancel any prior one
    timer = Process.send_after(self(), {:debounce, path}, state.debounce_ms)

    case state.paths[path] do
      nil ->
        state = put_in(state.paths[path], %{size: nil, mtime: nil, timer: timer})
        {:noreply, state}

      %{timer: old} ->
        Process.cancel_timer(old)
        state = put_in(state.paths[path].timer, timer)
        {:noreply, state}
    end
  end

  # Stale debounce: the entry was already resolved/removed. Ignore.
  def handle_info({:debounce, path}, state) when not is_map_key(state.paths, path) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:debounce, path}, state) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime, size: size}} ->
        curr_size = state.paths[path].size
        curr_mtime = state.paths[path].mtime

        # If the file has not been modified after the debounce window expires,
        # it is stable. We can remove it from our state and tell the subscriber
        # that it is now stable.
        if size == curr_size && mtime == curr_mtime do
          send(state.subscriber, {:stable, path})
          state = %{state | paths: Map.delete(state.paths, path)}
          {:noreply, state}
        else
          # Otherwise, we must schedule another debounce.
          timer = Process.send_after(self(), {:debounce, path}, state.debounce_ms)
          state = put_in(state.paths[path], %{size: size, mtime: mtime, timer: timer})

          {:noreply, state}
        end

      {:error, :enoent} ->
        send(state.subscriber, {:deleted, path})
        state = %{state | paths: Map.delete(state.paths, path)}
        {:noreply, state}

      {:error, reason} ->
        # If File.stat/2 fails for any other reason besides no entry, such as permissions
        # or a temporary lock, just delete the entry. We will just need to wait until another
        # file system event creates a new watch path.
        Logger.warning("File.stat/2 failed for reason #{inspect(reason)}")
        state = %{state | paths: Map.delete(state.paths, path)}
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:file_event, _worker_pid, :stop}, state) do
    # The filesystem worker is telling the Watcher its watch has ended (the worker is dead).
    # Stop the GenServer and let the supervisor restart it.
    {:stop, :filesystem_worker_stopped, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("SyncSet.Watcher got unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  def start_link(opts) do
    # The split peels :name if present into start_opts and leaves everything else in
    # init_opts. This keeps :name out of what init receives.
    {start_opts, init_opts} = Keyword.split(opts, [:name])
    # Allow the supervisor to handle errors -- just return whatever GenServer.start_link/3 does
    GenServer.start_link(__MODULE__, init_opts, start_opts)
  end
end
