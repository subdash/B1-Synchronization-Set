defmodule SyncSet.DataPlane do
  @moduledoc """
  When a node hears that a file changed and wants to sync its version with the
  remote node, it opens a TCP connection to the peer and pulls the file contents.

  Every node runs a TCP listener on a known port, so that when the peer connects
  it can stream the bytes over the socket in chunks and close the connection.

  On the client side of the data plane, the node that wants the file connects to
  the listener, sends a `{:get, path}` message and reads the incoming chunks. It
  does not write them straight to the real filename, but rather to a `.tmp`
  extension file.

  Once the transfer hits EOF, the client checksums the tmp file and compares it
  to the checksum from the metadata. If it matches, the tmp file is renamed to
  the original name of the file. By updating the index first (`apply_remote`)
  then renaming, by the time the watcher event arrives the index already knows
  about it and the echo suppression logic drops it.
  """
  use GenServer

  alias SyncSet.Index
  require Logger

  # The size of each chunk sent to clients, currently 64 KB
  @chunk_size 64 * 1024

  @enforce_keys [:listen_socket, :port, :sync_dir, :index]
  defstruct [:listen_socket, :port, :sync_dir, :index, :acceptor]

  def start_link(opts) do
    {start_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, start_opts)
  end

  @impl true
  def init(opts) do
    # The TCP port to listen on. Comes from runtime config, but we must allow passing
    # it as an arg for tests.
    port = Keyword.fetch!(opts, :port)
    # The directory from which we create an absolute path using the file path.
    sync_dir = Keyword.fetch!(opts, :sync_dir)
    # Needed for the orphan-.tmp sweep so it can ask if the .tmp files should be
    # removed or not.
    index = Keyword.fetch!(opts, :index)

    with {:ok, listen_socket} <-
           :gen_tcp.listen(port, [
             # :binary => deliver data as binaries, not char lists
             # packet: 4 => 4-byte length-prefix framing -- runtime adds the prefix on send
             # and strips it on recv
             # active: false => passive mode; pass 0 for length in recv to get one complete packet
             # reuseaddr: true => re-bind the port immediately after the listener restarts
             :binary,
             packet: 4,
             active: false,
             reuseaddr: true
           ]),
         {:ok, bound_port} <- :inet.port(listen_socket) do
      # acceptor is set later, in handle_continue(:startup, state)
      state = %__MODULE__{
        listen_socket: listen_socket,
        port: bound_port,
        sync_dir: sync_dir,
        index: index
      }

      {:ok, state, {:continue, :startup}}
    end
  end

  @impl true
  def handle_continue(:startup, state) do
    %__MODULE__{listen_socket: listen_socket, sync_dir: sync_dir, index: index} = state

    {:ok, pid} = Task.start_link(fn -> accept_loop(listen_socket, sync_dir) end)

    :ok = sweep_orphans(sync_dir, index)

    {:noreply, %{state | acceptor: pid}}
  end

  # Exposed for testing
  def sweep_orphans(sync_dir, index) do
    # NOTE: STARTUP ONLY. Otherwise, we could end up deleting an in-flight partial
    # .tmp file that we actually want.
    # Gather all .tmp files in the sync dir and child dirs
    Path.wildcard(Path.join(sync_dir, "**/*.tmp"))
    |> Enum.filter(fn abs_path ->
      # The index key
      relative_path = Path.relative_to(abs_path, sync_dir)
      # We want to delete only .tmp files that are NOT in the index
      is_nil(Index.get(index, relative_path))
    end)
    # Remove them from disk
    |> Enum.each(&File.rm/1)

    :ok
  end

  defp accept_loop(listen_socket, sync_dir) do
    # This function calls itself since each accept/1 call accepts only a single connection.
    # Subsequent calls block until a new peer connects. This works because we delegate file
    # serving to the handler spawned in a child process that we create here but do not await.
    case :gen_tcp.accept(listen_socket) do
      {:ok, conn} ->
        # Start handler
        {:ok, handler_pid} =
          Task.Supervisor.start_child(
            SyncSet.TransferSupervisor,
            fn -> handler(conn, sync_dir) end
          )

        # Transfer ownership of the socket to the handler pid so that it takes down the
        # socket with it. This is preferred so that we don't leak sockets if the handler crashes.
        # NOTE: Return value ignored from this call -- if it returns an error due to the handler
        # dying, the socket gets cleaned up anyway.
        :gen_tcp.controlling_process(conn, handler_pid)

        # Recursively run the accept loop
        accept_loop(listen_socket, sync_dir)

      {:error, :closed} ->
        # listen socket closed -- the listener is shutting down
        :ok

      {:error, reason} when reason in [:econnaborted, :timeout] ->
        # These errors don't warrant tearing down the listen socket
        Logger.warning("transient accept error: #{inspect(reason)}; continuing")
        accept_loop(listen_socket, sync_dir)

      {:error, reason} ->
        # unexpected -- crash so the supervisor rebuilds the listener
        raise "accept failed: #{inspect(reason)}"
    end
  end

  defp handler(conn, sync_dir) do
    # See note in init about passing 0 for length 
    {:ok, packet} = :gen_tcp.recv(conn, 0)
    {:get, file_path} = :erlang.binary_to_term(packet)
    absolute_path = Path.join(sync_dir, file_path)
    # Guard against path traversal
    expanded_sync_dir = Path.expand(sync_dir)
    expanded_file_path = Path.expand(absolute_path)

    case validate_file_path(expanded_file_path, expanded_sync_dir) do
      :ok ->
        # NOTE: Potential TOCTOU between checking the existence of the file in the
        # validate_file_path function and serving the file. If it gets deleted in between,
        # a crash occurs while streaming. We can let it crash because the socket will close
        # and the partial .tmp file will be discarded.
        serve_file(conn, absolute_path)

      error ->
        error_bytes = :erlang.term_to_binary(error)
        :gen_tcp.send(conn, error_bytes)
        :gen_tcp.close(conn)
    end
  end

  defp serve_file(conn, absolute_path) do
    :gen_tcp.send(conn, :erlang.term_to_binary(:ok))

    File.stream!(absolute_path, @chunk_size, [])
    # Utilize reduce while to break early in the case of an error
    |> Enum.reduce_while(:ok, fn chunk, _acc ->
      case :gen_tcp.send(conn, chunk) do
        :ok -> {:cont, :ok}
        {:error, _} = e -> {:halt, e}
      end
    end)

    :gen_tcp.close(conn)

    :ok
  end

  defp validate_file_path(expanded_file_path, expanded_sync_dir) do
    # NOTE: Design decision here: if a real file is requested that is not allowed (e.g.
    # outside the sync_dir or a symlink), still return :enoent. Avoid the existence oracle
    # so malicious users do not attempt to access files outside the syncronization set.

    # If the paths are equal or the expanded file path is located in the sync directory
    # (add the trailing slash to avoid matching prefixes), then we know we will only
    # be serving the client files from the sync_dir and not outside of it.
    case File.lstat(expanded_file_path) do
      {:ok, %File.Stat{type: :symlink}} ->
        # TODO: Enable local symlinks
        # We could follow the symlink to ensure it does not link outside of the sync dir,
        # but a much simpler and less error prone implementation is to just forbid symlinks
        # in the sync directory.
        {:error, :enoent}

      {:ok, %{type: _}} ->
        if within_sync_dir?(expanded_file_path, expanded_sync_dir) do
          :ok
        else
          Logger.warning(
            "Client attempted to pull an existing file outside of the sync directory: #{expanded_file_path}"
          )

          {:error, :enoent}
        end

      {:error, _} ->
        {:error, :enoent}
    end
  end

  defp within_sync_dir?(path, root), do: path == root or String.starts_with?(path, root <> "/")
end
