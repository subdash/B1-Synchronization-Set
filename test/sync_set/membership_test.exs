defmodule SyncSet.MembershipTest do
  use ExUnit.Case
  alias SyncSet.Membership

  describe "topology_from_env/1" do
    test "missing key defaults to gossip" do
      topology = Membership.topology_from_env(%{})
      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Gossip
      assert Keyword.fetch!(topology, :config) == []
    end

    test "explicit gossip" do
      topology = Membership.topology_from_env(%{"SYNC_CLUSTER_STRATEGY" => "gossip"})
      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Gossip
      assert Keyword.fetch!(topology, :config) == []
    end

    test "happy-path parse + atom conversion" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd",
          "SYNC_CLUSTER_HOSTS" => "a@h1,b@h2"
        })

      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Epmd
      assert Keyword.fetch!(topology, :config) == [{:hosts, [:a@h1, :b@h2]}]
    end

    test "missing hosts -> empty list, not crash" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd"
        })

      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Epmd
      assert Keyword.fetch!(topology, :config) == [{:hosts, []}]
    end

    test "empty string -> []" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd",
          "SYNC_CLUSTER_HOSTS" => ""
        })

      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Epmd
      assert Keyword.fetch!(topology, :config) == [{:hosts, []}]
    end

    test "trailing comma dropped (trim: true)" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd",
          "SYNC_CLUSTER_HOSTS" => "a@h1,"
        })

      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Epmd
      assert Keyword.fetch!(topology, :config) == [{:hosts, [:a@h1]}]
    end

    test "single host" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd",
          "SYNC_CLUSTER_HOSTS" => "a@h1"
        })

      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Epmd
      assert Keyword.fetch!(topology, :config) == [{:hosts, [:a@h1]}]
    end

    test "unknown strategy fails" do
      assert_raise RuntimeError, fn ->
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "3pmd",
          "SYNC_CLUSTER_HOSTS" => "a@h1"
        })
      end
    end

    test "strategies are case-insensitive" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "EPMD",
          "SYNC_CLUSTER_HOSTS" => "a@h1"
        })

      assert Keyword.fetch!(topology, :strategy) == Cluster.Strategy.Epmd
    end

    test "each host name is trimmed" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd",
          "SYNC_CLUSTER_HOSTS" => "a@h1, b@h2"
        })

      assert Keyword.fetch!(topology, :config) == [{:hosts, [:a@h1, :b@h2]}]
    end

    test "stray empty hosts are rejected" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd",
          "SYNC_CLUSTER_HOSTS" => "a@h1, ,b@h2"
        })

      assert Keyword.fetch!(topology, :config) == [{:hosts, [:a@h1, :b@h2]}]
    end

    test "trailing commas in host list are rejected" do
      topology =
        Membership.topology_from_env(%{
          "SYNC_CLUSTER_STRATEGY" => "epmd",
          "SYNC_CLUSTER_HOSTS" => "a@h1, "
        })

      assert Keyword.fetch!(topology, :config) == [{:hosts, [:a@h1]}]
    end
  end
end
