defmodule SyncSet.NodeHarness do
  defstruct [:name, :pid, :sync_dir, :dets_path]

  import ExUnit.Callbacks

  # Not System.tmp_dir!/0: on macOS it returns /var/... which is a symlink to
  # /private/var/..., and fsevents reports the resolved path. Watcher events would then
  # not match sync_dir, and Path.relative_to/2 would hand back an absolute key.
  # File.cwd!/0 comes from getcwd(3), which is already symlink-free.
  def tmp_base, do: Path.join(File.cwd!(), "tmp/syncset_test")

  def spawn_node(label) do
    # mint identity + paths
    uniq = System.unique_integer([:positive])
    base = tmp_base()
    sync_dir = Path.join(base, "#{label}_#{uniq}")
    dets_path = Path.join(base, "#{label}_#{uniq}.dets")
    File.mkdir_p!(base)
    # start peer (unlinked) + add code paths
    {:ok, pid, node} =
      :peer.start(%{
        name: :"#{label}_#{uniq}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", ~c"syncset_test"]
      })

    :erpc.call(node, :code, :add_paths, [:code.get_path()])
    # Inject config - must be before ensure_all started
    :erpc.call(node, Application, :put_env, [:sync_set, :sync_dir, sync_dir])
    :erpc.call(node, Application, :put_env, [:sync_set, :dets_path, dets_path])
    :erpc.call(node, Application, :put_env, [:sync_set, :data_port, 0])
    :erpc.call(node, Application, :put_env, [:sync_set, :start_runtime?, true])
    # empty hosts => no-op for now
    :erpc.call(node, System, :put_env, ["SYNC_CLUSTER_STRATEGY", "epmd"])
    # boot the app tree
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:sync_set])
    # register teardown
    on_exit(fn ->
      :peer.stop(pid)
      File.rm_rf!(sync_dir)
      File.rm_rf!(dets_path)
    end)

    %SyncSet.NodeHarness{name: node, pid: pid, sync_dir: sync_dir, dets_path: dets_path}
  end

  def write_file(node, rel_path, contents),
    do: File.write!(Path.join(node.sync_dir, rel_path), contents)

  def delete_file(node, rel_path), do: File.rm!(Path.join(node.sync_dir, rel_path))

  def read_index(node),
    do: :erpc.call(node.name, SyncSet.Index, :snapshot, [SyncSet.Index]) |> Enum.sort()

  def list_files(node) do
    node.sync_dir
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.reject(&String.ends_with?(&1, ".tmp"))
    |> Enum.map(&Path.relative_to(&1, node.sync_dir))
    |> Enum.sort()
  end

  def file_checksum(node, rel_path),
    do: SyncSet.Checksum.of_file(Path.join(node.sync_dir, rel_path))

  def assert_converged(node_a, node_b, opts \\ []) do
    SyncSet.Wait.wait_until(
      fn ->
        read_index(node_a) == read_index(node_b) and
          list_files(node_a) == list_files(node_b) and
          list_files(node_a)
          |> Enum.all?(fn path ->
            file_checksum(node_a, path) == file_checksum(node_b, path)
          end)
      end,
      opts
    )
  end

  def assert_tombstoned(node, rel_path) do
    SyncSet.Wait.wait_until(fn ->
      case :erpc.call(node.name, SyncSet.Index, :get, [SyncSet.Index, rel_path]) do
        nil ->
          false

        %{deleted: tombstoned} ->
          rel_path not in list_files(node) and tombstoned == true
      end
    end)
  end

  def assert_conflict_copy(node, rel_path, content_a, content_b) do
    cs_a = SyncSet.Checksum.of_binary(content_a)
    cs_b = SyncSet.Checksum.of_binary(content_b)
    # Conflict.winner rule: larger checksum wins => loser is smaller
    {_loser_content, loser_cs} = if cs_a < cs_b, do: {content_a, cs_a}, else: {content_b, cs_b}

    expected = SyncSet.Conflict.conflict_path(rel_path, loser_cs)

    SyncSet.Wait.wait_until(fn ->
      expected in list_files(node) and file_checksum(node, expected) == {:ok, loser_cs}
    end)
  end

  # Call :nodeup on both sides. The Reconciler runs its exchange, and anti-entropy converges them.
  # For live propagation (writing a file after connecting), Control also sees the peer in Node.list()
  # and announces.
  def connect(node_a, node_b), do: true = :erpc.call(node_a.name, Node, :connect, [node_b.name])
end
