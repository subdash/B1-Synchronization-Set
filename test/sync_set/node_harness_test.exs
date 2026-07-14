defmodule SyncSet.NodeHarnessTest do
  # peers bind ports -- never async
  use SyncSet.ClusterCase, async: false

  test "peer boots and is reachable over erpc" do
    node = spawn_node("a")
    # Index registered and alive => runtime tree came up
    assert :erpc.call(node.name, Process, :whereis, [SyncSet.Index]) |> is_pid()
    # DataPlane bound a real ephemeral port => data_port: 0 resolved
    port = :erpc.call(node.name, SyncSet.DataPlane, :get_port, [SyncSet.DataPlane])
    assert is_integer(port) and port > 0
  end

  test "two nodes converge on a create" do
    a = spawn_node("a")
    b = spawn_node("b")
    connect(a, b)
    write_file(a, "hello.txt", "hi there")
    assert_converged(a, b, timeout_ms: 5000)
    assert file_checksum(b, "hello.txt") == {:ok, SyncSet.Checksum.of_binary("hi there")}
  end
end
