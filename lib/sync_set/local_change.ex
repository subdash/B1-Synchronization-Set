defmodule SyncSet.LocalChange do
  alias SyncSet.{Checksum, Entry, Index}
  require Logger

  use GenServer

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    control = Keyword.fetch!(opts, :control)
    sync_dir = Keyword.fetch!(opts, :sync_dir)

    state = %{
      index: index,
      control: control,
      sync_dir: sync_dir
    }

    {:ok, state}
  end

  def start_link(opts) do
    {start_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, start_opts)
  end

  @impl true
  def handle_info({:stable, path}, state) do
    # The Watcher reports an absolute path; read from it, but key the Index by the
    # sync_dir-relative path so keys are portable across nodes (see Scanner, which
    # derives keys the same way).
    with {:ok, binary} <- File.read(path) do
      size = byte_size(binary)
      checksum = Checksum.of_binary(binary)
      key = Path.relative_to(path, state.sync_dir)

      case Index.get(state.index, key) do
        %Entry{size: ^size, checksum: ^checksum} ->
          {:noreply, state}

        _ ->
          Index.local_write(state.index, key, checksum, size)
          send(state.control, {:local_change, key})
          {:noreply, state}
      end
    else
      {:error, reason} ->
        Logger.warning("LocalChange could not read #{path}: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:deleted, path}, state) do
    key = Path.relative_to(path, state.sync_dir)

    case Index.get(state.index, key) do
      %Entry{deleted: false} ->
        Index.local_delete(state.index, key)
        send(state.control, {:local_change, key})
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message in LocalChange: #{inspect(msg)}")
    {:noreply, state}
  end
end
