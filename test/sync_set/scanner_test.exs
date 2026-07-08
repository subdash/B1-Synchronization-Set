defmodule SyncSet.ScannerTest do
  use ExUnit.Case, async: false
  alias SyncSet.{Checksum, DataPlane, Index, Scanner}

  # ExUnit wipes tmp_dir before each test, so nothing leaks across runs.
  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    # Unique names/paths so the DETS-backed Index doesn't collide across tests.
    uniq = System.unique_integer([:positive])

    sync_dir = Path.join(tmp_dir, "sync_dir")
    :ok = File.mkdir_p!(sync_dir)

    index_name = String.to_atom("scanner_index_#{uniq}")

    index_opts = [
      dets_path: Path.join(tmp_dir, "scanner_idx_#{uniq}.dets"),
      table: String.to_atom("scanner_table_#{uniq}"),
      name: index_name
    ]

    start_supervised!({Index, index_opts})

    # The Scanner announces by sending {:local_change, key} to its control ref.
    # Using the test process as `control` lets tests assert_receive / refute_receive
    # on those messages directly — no fake Control process needed.
    #
    # `sweep_orphans/2` is a plain function on DataPlane, so the module itself is
    # the seam; drop `data_plane` here if the Scanner calls DataPlane directly.
    %{
      sync_dir: sync_dir,
      index: index_name,
      control: self(),
      data_plane: DataPlane
    }
  end

  describe "scan/cold start" do
    test "a brand-new file is written to the index and announced",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      File.write!(Path.join(sync_dir, "greeting.txt"), contents)

      # Scanner scans on boot: hashes each file, writes unknown ones to the
      # Index, and announces them to `control` as {:local_change, key}.
      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      assert_receive {:local_change, "greeting.txt"}

      # The file is now a known local entry with the correct checksum and size.
      {:ok, expected_checksum} = Checksum.of_file(Path.join(sync_dir, "greeting.txt"))
      entry = Index.get(index, "greeting.txt")
      assert entry.checksum == expected_checksum
      assert entry.size == byte_size(contents)
    end

    test "a known and unchanged file does not announce",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      File.write!(Path.join(sync_dir, "greeting.txt"), contents)
      {:ok, expected_checksum} = Checksum.of_file(Path.join(sync_dir, "greeting.txt"))
      Index.local_write(index, "greeting.txt", expected_checksum, 11)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      refute_receive {:local_change, "greeting.txt"}
    end

    test "the scanner announces an update for a file modified offline",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      File.write!(Path.join(sync_dir, "greeting.txt"), contents)
      {:ok, expected_checksum} = Checksum.of_file(Path.join(sync_dir, "greeting.txt"))
      # Write a bogus checksum and assert the change is announced
      Index.local_write(index, "greeting.txt", "asdf", 11)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      assert_receive {:local_change, "greeting.txt"}
      entry = Index.get(index, "greeting.txt")
      assert entry.checksum == expected_checksum
    end

    test "the scanner scans files at nested paths",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      nested_path = Path.join(sync_dir, "tmp_dir")
      File.mkdir_p!(nested_path)
      File.write!(Path.join(nested_path, "greeting.txt"), contents)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      assert_receive {:local_change, "tmp_dir/greeting.txt"}
      assert Index.get(index, "tmp_dir/greeting.txt") != nil
    end

    test "the scanner scans files at mixed depths",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      nested_path = Path.join(sync_dir, "tmp_dir")
      File.mkdir_p!(nested_path)
      File.write!(Path.join(sync_dir, "greeting.txt"), contents)
      File.write!(Path.join(nested_path, "greeting.txt"), contents)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      assert_receive {:local_change, "greeting.txt"}
      assert_receive {:local_change, "tmp_dir/greeting.txt"}
      assert Index.get(index, "tmp_dir/greeting.txt") != nil
      assert Index.get(index, "greeting.txt") != nil
    end

    test "the scanner scans dot files",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      File.write!(Path.join(sync_dir, ".env"), contents)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      assert_receive {:local_change, ".env"}
      assert Index.get(index, ".env") != nil
    end

    test "the scanner does not scan .tmp files and they are orphan swept",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      file_path = Path.join(sync_dir, "file.tmp")
      File.write!(file_path, contents)

      assert File.exists?(file_path)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      # File is not announced, nor indexed, and it is swept after the scan
      refute_receive {:local_change, "file.tmp"}
      assert Index.get(index, "file.tmp") == nil
      refute File.exists?(file_path)
    end

    test "the scanner does not index, announce or sweep directories",
         %{sync_dir: sync_dir, index: index, control: control} do
      File.mkdir_p!(Path.join(sync_dir, "subdir"))

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      refute_receive {:local_change, "subdir"}
      assert Index.get(index, "subdir") == nil
      assert File.exists?(Path.join(sync_dir, "subdir"))
    end

    test "file in index, absent from disk, is not deleted from index",
         %{sync_dir: sync_dir, index: index, control: control} do
      Index.local_write(index, "greeting.txt", "asdf", 11)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      refute_receive {:local_change, "greeting.txt"}
    end

    test "the actual file size is written to the index",
         %{sync_dir: sync_dir, index: index, control: control} do
      contents = "hello world"
      file_path = Path.join(sync_dir, "greeting.txt")
      File.write!(file_path, contents)
      %{size: known_size} = File.stat!(file_path)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      assert_receive {:local_change, "greeting.txt"}

      entry = Index.get(index, "greeting.txt")
      assert entry.size == known_size
    end

    test "empty dir -- sweep stil runs clean",
         %{sync_dir: sync_dir, index: index, control: control} do
      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane}
      )

      refute_receive {:local_change, _}
      assert Index.snapshot(index) == %{}
    end

    test "scan is idempotent",
         %{sync_dir: sync_dir, index: index, control: control} do
      # Typical setup, just assert that the first scan produces the expected results.
      contents = "hello world"
      File.write!(Path.join(sync_dir, "greeting.txt"), contents)

      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane},
        id: :scan_run_1
      )

      assert_receive {:local_change, "greeting.txt"}

      {:ok, expected_checksum} = Checksum.of_file(Path.join(sync_dir, "greeting.txt"))
      entry = Index.get(index, "greeting.txt")
      assert entry.checksum == expected_checksum
      assert entry.size == byte_size(contents)

      # Force a second scan
      start_supervised!(
        {Scanner, sync_dir: sync_dir, index: index, control: control, data_plane: DataPlane},
        id: :scan_run_2
      )

      refute_receive {:local_change, _}

      entry = Index.get(index, "greeting.txt")
      assert entry.checksum == expected_checksum
      assert entry.size == byte_size(contents)
    end
  end
end
