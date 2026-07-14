defmodule SyncSet.Wait do
  def wait_until(fun, opts \\ []) do
    timeout = Keyword.get(opts, :timeout_ms, 2_000)
    interval = Keyword.get(opts, :interval_ms, 25)
    deadline = System.monotonic_time(:millisecond) + timeout
    message = Keyword.get(opts, :message)

    loop(fun, timeout, interval, deadline, message)
  end

  defp loop(fun, timeout, interval, deadline, message) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "wait_until timed out after #{timeout}ms" <> maybe(message)
        else
          Process.sleep(interval)
          loop(fun, timeout, interval, deadline, message)
        end

      truthy ->
        truthy
    end
  end

  defp maybe(nil), do: ""

  defp maybe(msg), do: " " <> msg
end
