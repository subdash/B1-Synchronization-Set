defmodule SyncSet.Watcher.Backend.Fake do
  @behaviour SyncSet.Watcher.Backend

  @impl true
  def watch(_dirs) do
    # Create a process that sleeps forever and link it to the Watcher process so that
    # when the Watcher dies the linked process dies with it (and doesn't hang around after
    # tests).
    pid = spawn_link(fn -> Process.sleep(:infinity) end)
    {:ok, pid}
  end
end
