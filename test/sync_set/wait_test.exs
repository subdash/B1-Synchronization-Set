defmodule SyncSet.WaitTest do
  use ExUnit.Case, async: true
  import SyncSet.Wait

  test "already true -> returns the value immediately" do
    assert wait_until(fn -> :done end) == :done
  end

  test "flips true partway -> polls until it succeeds" do
    start = System.monotonic_time(:millisecond)
    result = wait_until(fn -> System.monotonic_time(:millisecond) - start > 50 end)
    assert result == true
    # Prove the polling loop
    elapsed = System.monotonic_time(:millisecond) - start
    assert elapsed >= 50
  end

  test "never true -> raises on timeout" do
    start = System.monotonic_time(:millisecond)

    assert_raise RuntimeError, ~r/after 100ms/, fn ->
      wait_until(fn -> false end, timeout_ms: 100)
    end

    elapsed = System.monotonic_time(:millisecond) - start
    assert elapsed > 90
    assert elapsed < 500
  end

  test "truthy passthrough for a non-boolean" do
    assert wait_until(fn -> {:ok, 42} end) == {:ok, 42}
  end

  test "custom :message shows up on timeout" do
    start = System.monotonic_time(:millisecond)

    assert_raise RuntimeError, ~r/my ctx/, fn ->
      wait_until(fn -> false end, timeout_ms: 100, message: "my ctx")
    end

    elapsed = System.monotonic_time(:millisecond) - start
    assert elapsed > 90
    assert elapsed < 500
  end
end
