defmodule SyncSet.ConflictTest do
  use ExUnit.Case

  describe "entry and conflict tests" do
    test "winner returns :left when left >= right" do
      left = %SyncSet.Entry{
        checksum: "bbcdefgh"
      }

      right = %SyncSet.Entry{
        checksum: "abcdefgh"
      }

      assert SyncSet.Conflict.winner(left, right) == :left
      assert SyncSet.Conflict.winner(left, left) == :left
    end

    test "winner returns :right when left < right" do
      left = %SyncSet.Entry{
        checksum: "abcdefgh"
      }

      right = %SyncSet.Entry{
        checksum: "bbcdefgh"
      }

      assert SyncSet.Conflict.winner(left, right) == :right
    end

    test "conflict returns the expected sync conflict filename" do
      path = "notes.txt"
      checksum = "abcdefghABCDEFGH"

      assert SyncSet.Conflict.conflict_path(path, checksum) ==
               "notes.sync-conflict-abcdefgh.txt"

      path = "sub/dir/notes.txt"

      assert SyncSet.Conflict.conflict_path(path, checksum) ==
               "sub/dir/notes.sync-conflict-abcdefgh.txt"

      path = "README"

      assert SyncSet.Conflict.conflict_path(path, checksum) ==
               "README.sync-conflict-abcdefgh"
    end
  end
end
