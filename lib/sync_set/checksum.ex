defmodule SyncSet.Checksum do
  def of_binary(data) do
    :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)
  end

  def of_file(path) do
    # If File.read/1 returns an error, with will fall through to
    # the error value returned from File.read/1, e.g. {:error, :enoent}
    with {:ok, binary} <- File.read(path), do: {:ok, of_binary(binary)}
  end
end
