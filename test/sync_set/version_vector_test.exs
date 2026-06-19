defmodule SyncSet.VersionVectorTest do
  alias SyncSet.VersionVector, as: VV
  use ExUnit.Case
  use ExUnitProperties

  describe "SyncSet.VersionVector.compare tests" do
    test "compare returns :equal for two identical vectors" do
      v1 = %{A: 1, B: 2}
      v2 = %{A: 1, B: 2}
      assert VV.compare(v1, v2) == :equal
    end

    test "compare returns dominates when a >= b" do
      v1 = %{A: 2, B: 3}
      v2 = %{A: 1, B: 2}
      assert VV.compare(v1, v2) == :dominates
    end

    test "compare returns dominated when b >= a" do
      v1 = %{A: 1, B: 2}
      v2 = %{A: 2, B: 3}
      assert VV.compare(v1, v2) == :dominated
    end

    test "compare returns concurrent when no other predicate matches" do
      v1 = %{A: 1, B: 3}
      v2 = %{A: 3, B: 1}
      assert VV.compare(v1, v2) == :concurrent
    end

    test "anything dominates %{}" do
      v1 = %{A: 1, B: 2}
      v2 = VV.new()
      assert VV.compare(v1, v2) == :dominates
      assert VV.compare(v2, v1) == :dominated
    end

    test "two empty vectors are equal" do
      assert VV.compare(VV.new(), VV.new()) == :equal
    end

    property "comparison is symmetric/antisymmetric" do
      check all(
              v1 <- version_vector(),
              v2 <- version_vector()
            ) do
        case VV.compare(v1, v2) do
          :dominates ->
            assert VV.compare(v2, v1) == :dominated

          :dominated ->
            assert VV.compare(v2, v1) == :dominates

          :equal ->
            assert VV.compare(v2, v1) == :equal

          :concurrent ->
            assert VV.compare(v2, v1) == :concurrent
        end
      end
    end
  end

  describe "SyncSet.VersionVector.increment tests" do
    test "increment adds 1 to the target node's count" do
      v1 = %{A: 1, B: 2}
      v1 = VV.increment(v1, :A)
      assert v1[:A] == 2
    end

    property "a local write always moves you forward" do
      check all(vv <- version_vector()) do
        assert VV.compare(VV.increment(vv, :A), vv) == :dominates
      end
    end

    test "incrementing an empty vector creates the node with a value of 1" do
      v1 = VV.new()
      assert VV.increment(v1, :A) == %{A: 1}
    end

    test "incrementing on a missing node likewise creates the node with value of 1" do
      v1 = %{A: 1}
      assert VV.increment(v1, :B) == %{A: 1, B: 1}
    end
  end

  describe "SyncSet.VersionVector.merge tests" do
    test "merging two identical vectors results in the same vector" do
      v1 = %{A: 1, B: 2}
      assert VV.merge(v1, v1) == v1
    end

    test "merging two vectors with different keys combines them" do
      v1 = %{A: 1, B: 2}
      v2 = %{C: 1, D: 2}
      expected = %{A: 1, B: 2, C: 1, D: 2}

      assert VV.merge(v1, v2) == expected
      assert VV.merge(v2, v1) == expected
    end

    test "merging two vectors with overlapping keys favors the vector with the higher value for conflicts" do
      v1 = %{A: 1, B: 2}
      v2 = %{A: 1, B: 3}

      assert VV.merge(v1, v2) == %{A: 1, B: 3}
      assert VV.merge(v2, v1) == %{A: 1, B: 3}
    end

    property "merge is idempotent" do
      check all(vv <- version_vector()) do
        assert VV.merge(vv, vv) == vv
      end
    end

    property "merge is commutative" do
      check all(
              v1 <- version_vector(),
              v2 <- version_vector()
            ) do
        assert VV.merge(v1, v2) == VV.merge(v2, v1)
      end
    end

    property "merge is associative" do
      check all(
              v1 <- version_vector(),
              v2 <- version_vector(),
              v3 <- version_vector()
            ) do
        assert VV.merge(VV.merge(v1, v2), v3) == VV.merge(v1, VV.merge(v2, v3))
      end
    end
  end

  defp version_vector do
    map_of(member_of([:A, :B, :C, :D, :E, :F, :G, :H, :I, :J]), integer(0..8), max_length: 3)
  end
end
