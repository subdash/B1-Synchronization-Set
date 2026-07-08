defmodule SyncSet.Scanner do
  use GenServer
  alias SyncSet.{Checksum, DataPlane, Entry, Index}
  require Logger

  def start_link(opts) do
    {start_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, start_opts)
  end

  @impl true
  def init(opts) do
    sync_dir = Keyword.fetch!(opts, :sync_dir)
    index = Keyword.fetch!(opts, :index)
    control = Keyword.fetch!(opts, :control)

    state = %{
      sync_dir: sync_dir,
      index: index,
      control: control
    }

    {:ok, state, {:continue, :scan}}
  end

  @impl true
  def handle_continue(:scan, state) do
    DataPlane.sweep_orphans(state.sync_dir, state.index)

    Path.join(state.sync_dir, "**")
    # Use Path.wildcard/2 to match every descendant at any depth. Use the match_dot
    # option to sync hidden files.
    |> Path.wildcard(match_dot: true)
    # Drop directories -- we'll traverse them, but don't treat them as files
    |> Enum.filter(&File.regular?/1)
    # Skip in flight .tmp files
    |> Enum.reject(&String.ends_with?(&1, ".tmp"))
    # For each file, if the index checksum does not match the computed checksum,
    # announce a file change to the control plane.
    |> Enum.each(fn full -> scan_file(full, state) end)

    {:noreply, state}
  end

  defp scan_file(full, state) do
    # Compute checksum and compare against index. If they differ, perform a local index write
    # and announce a change to the control plane.
    key = Path.relative_to(full, state.sync_dir)

    with {:ok, bin} <- File.read(full),
         checksum = Checksum.of_binary(bin),
         :changed <- classify(Index.get(state.index, key), checksum) do
      Index.local_write(state.index, key, checksum, byte_size(bin))
      send(state.control, {:local_change, key})
    else
      :unchanged -> :ok
      {:error, reason} -> Logger.warning("scanner: skipping #{key}: #{inspect(reason)}")
    end
  end

  # The entry checksum matches the checksum in the second position -> :unchanged
  defp classify(%Entry{checksum: same}, same), do: :unchanged
  defp classify(_entry, _checksum), do: :changed
end
