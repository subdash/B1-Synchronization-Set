defmodule SyncSet.LocalChange do
  alias SyncSet.{Checksum, Entry, Index}
  require Logger

  use GenServer

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    control = Keyword.fetch!(opts, :control)

    state = %{
      index: index,
      control: control
    }

    {:ok, state}
  end

  def start_link(opts) do
    {start_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, start_opts)
  end

  @impl true
  def handle_info({:stable, path}, state) do
    with {:ok, binary} <- File.read(path) do
      size = byte_size(binary)
      checksum = Checksum.of_binary(binary)

      case Index.get(state.index, path) do
        %Entry{size: ^size, checksum: ^checksum} ->
          {:noreply, state}

        _ ->
          Index.local_write(state.index, path, checksum, size)
          send(state.control, {:announce, path})
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
    case Index.get(state.index, path) do
      %Entry{deleted: false} ->
        Index.local_delete(state.index, path)
        send(state.control, {:announce, path})
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
