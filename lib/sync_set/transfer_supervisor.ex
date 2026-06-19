defmodule SyncSet.TransferSupervisor do
  @moduledoc """
  A pure module exposing `start_transfer/3` so that peers can pull file contents
  when changes are announced.
  """
  alias SyncSet.{Checksum, Config, Index}
  require Logger
  @socket_opts [:binary, packet: 4, active: false, reuseaddr: true]

  def start_transfer(peer, file_path, entry) do
    # Start a child task that pulls the file contents. All of the magic happens in pull/6.
    # start_child will either return {:ok, pid} or {:error, error}, so it is up to the caller
    # to log or handle the error. However such an error likely indicates a configuration error,
    # so once this is production ready that should not happen.
    pull_closure = fn -> pull(peer.host, peer.port, file_path, entry, Config.sync_dir()) end
    Task.Supervisor.start_child(SyncSet.TransferSupervisor, pull_closure)
  end

  # Exposed publicly for ease of testing
  def pull(host, port, file_path, entry, sync_dir, index \\ SyncSet.Index) do
    # What we're doing here at the transport layer:
    # connect: Create socket on host:port address
    # send: send request to peer to get a file's contents
    # recv: receive the first packet, which tells this client whether it is allowed to read the file
    with {:ok, socket} <- :gen_tcp.connect(host, port, @socket_opts),
         :ok <- :gen_tcp.send(socket, :erlang.term_to_binary({:get, file_path})),
         {:ok, packet} <- :gen_tcp.recv(socket, 0),
         :ok <- :erlang.binary_to_term(packet) do
      # Fetch file contents and write to tmp_path.
      tmp_path = Path.join(sync_dir, file_path <> ".tmp")
      tmp_file_checksum = fetch(tmp_path, socket)
      # Compare checksums and handle result. We can ignore the return value since it is
      # either an indication of success (:ok) or a distinct error (:checksum_mismatch).
      verify_and_land(index, entry, file_path, tmp_path, tmp_file_checksum, sync_dir)
    else
      {:error, reason} ->
        # If that first packet matches this case clause, there is no file data to read.
        Logger.warning("TransferSupervisor pull failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp fetch(tmp_path, conn) do
    # In case the file path provided is in a directory that does not exist, create it
    :ok = File.mkdir_p(Path.dirname(tmp_path))
    # Open file in write mode and then return the checksum from our helper function
    {:ok, file} = File.open(tmp_path, [:write, :binary])
    read_socket_write_file(tmp_path, file, conn)
  end

  defp read_socket_write_file(tmp_path, file, conn) do
    # Loop recv and write binary contents to the tmp file. Checksum the tmp file when done
    # and return it to fetch/2.
    # NOTE: recv errors other than :closed have no clause and will raise a CaseClauseError.
    # This is acceptable due to the transient nature of the task.
    case :gen_tcp.recv(conn, 0) do
      {:ok, chunk} ->
        :ok = IO.binwrite(file, chunk)
        read_socket_write_file(tmp_path, file, conn)

      {:error, :closed} ->
        :ok = File.close(file)
        {:ok, checksum} = Checksum.of_file(tmp_path)
        checksum
    end
  end

  defp verify_and_land(index, entry, file_path, tmp_path, tmp_file_checksum, sync_dir) do
    # If checksums match, rename (overwrite) the file on disk though only after updating the
    # local index to prevent an echo loop. If they do not match, remove the tmp file and return
    # an error.
    if tmp_file_checksum == entry.checksum do
      Index.apply_remote(index, file_path, entry)
      :ok = File.rename(tmp_path, Path.join(sync_dir, file_path))
    else
      File.rm(tmp_path)
      {:error, :checksum_mismatch}
    end
  end
end
