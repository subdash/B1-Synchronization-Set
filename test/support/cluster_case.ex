# Case template for distribution tests
defmodule SyncSet.ClusterCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      # This code actually runs in the test module, making these available anywhere
      # that we use SyncSet.ClusterCase
      import SyncSet.NodeHarness
      import SyncSet.Wait
    end
  end

  setup_all do
    # This only runs in the template's setup lifecycle, rather than in each test
    if Node.self() == :nonode@nohost do
      case :net_kernel.start(:"manager@127.0.0.1", %{name_domain: :longnames}) do
        {:ok, _pid} -> :ok
        {:error, reason} -> raise "couldn't start distribution: #{inspect(reason)}"
      end
    end

    Node.set_cookie(:syncset_test)
    :ok
  end
end
