defmodule SyncSet.ChecksumTest do
  use ExUnit.Case
  alias SyncSet.Checksum

  @known_hash "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

  test "of_binary(hello) equals the known SHA-256 hex" do
    assert Checksum.of_binary("hello") == @known_hash
  end

  test "of_binary/1 produces different outputs for different inputs" do
    assert Checksum.of_binary("Hello") != @known_hash
  end

  @tag :tmp_dir
  test "of_file/1 returns hash of file", %{tmp_dir: tmp_dir} do
    file_path = Path.join(tmp_dir, "tmp_file")
    :ok = File.write!(file_path, "hello")
    {:ok, file_checksum} = Checksum.of_file(file_path)
    assert file_checksum == @known_hash
  end

  @tag :tmp_dir
  test "of_file/1 returns error on missing path", %{tmp_dir: tmp_dir} do
    file_path = Path.join(tmp_dir, "tmp_file2")
    # No file there
    {:error, reason} = Checksum.of_file(file_path)
    assert reason == :enoent
  end
end
