defmodule SyncSet.Watcher.Backend do
  @callback watch(dirs :: list(String.t())) :: {:ok, pid()}
end
