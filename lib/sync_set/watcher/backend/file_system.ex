defmodule SyncSet.Watcher.Backend.FileSystem do
  @behaviour SyncSet.Watcher.Backend

  @impl true
  def watch(dirs) do
    # Links the worker to the Watcher when called from inside the
    # Watcher's init. If the worker crashes, it takes the Watcher down too.
    # The supervisor then restarts the Watcher GenServer.
    {:ok, worker_pid} = FileSystem.start_link(dirs: dirs)
    # This registers the current process as a subscriber of the file system
    # worker (the process spawned by start_link above).
    :ok = FileSystem.subscribe(worker_pid)
    {:ok, worker_pid}
  end
end
