defmodule SyncSet.WatcherTest do
  use ExUnit.Case, async: true

  describe "Backend tests with fake watcher" do
    setup do
      opts = [
        backend: SyncSet.Watcher.Backend.Fake,
        # self() == test process's pid. By injecting it into the watcher (below
        # in start_supervised!), the watcher sends its messages to the test process's
        # pid. When we call assert_receive in a test, it reads the mailbox of the test
        # process.
        subscriber: self(),
        sync_dir: "fake/dir",
        debounce_ms: 50
      ]

      watcher_pid = start_supervised!({SyncSet.Watcher, opts})
      {:ok, watcher_pid: watcher_pid}
    end

    @tag :tmp_dir
    test "a burst of events for one path -> exactly one {:stable, path}", %{
      watcher_pid: watcher_pid,
      tmp_dir: tmp_dir
    } do
      # Create a file
      file_path = Path.join(tmp_dir, "file.txt")
      File.write!(file_path, "asdf")
      # Mimic worker rapid firing changes for the created file
      for _ <- 0..10 do
        send(watcher_pid, {:file_event, self(), {file_path, [:modified]}})
      end

      # Assert one and only one :stable message was received
      assert_receive {:stable, ^file_path}, 500
      # Assert that no second :stable message arrived
      refute_receive {:stable, ^file_path}, 30
    end

    test "a path that disappears -> {:deleted, path}", %{
      watcher_pid: watcher_pid
    } do
      file_path = "not/a/real/path/to/file.txt"
      # Mimic worker rapid firing changes for the created file
      for _ <- 0..10 do
        send(watcher_pid, {:file_event, self(), {file_path, [:modified]}})
      end

      # Assert one and only one :deleted message was received
      assert_receive {:deleted, ^file_path}, 500
      # Assert that no second :deleted message arrived
      refute_receive {:deleted, ^file_path}, 30
    end

    @tag :tmp_dir
    test "two different paths in flight at once don't interfere with each other's timers", %{
      watcher_pid: watcher_pid,
      tmp_dir: tmp_dir
    } do
      # Create two files
      file_path0 = Path.join(tmp_dir, "file0.txt")
      file_path1 = Path.join(tmp_dir, "file1.txt")
      File.write!(file_path0, "asdf")
      File.write!(file_path1, "fdsa")
      # Mimic worker rapid firing changes for the created files
      for _ <- 0..10 do
        send(watcher_pid, {:file_event, self(), {file_path0, [:modified]}})
        send(watcher_pid, {:file_event, self(), {file_path1, [:modified]}})
      end

      # Assert one and only one :stable message was received for each path
      assert_receive {:stable, ^file_path0}, 500
      assert_receive {:stable, ^file_path1}, 500
      # Assert that no second :stable message arrived (again, for each path)
      refute_receive {:stable, ^file_path0}, 30
      refute_receive {:stable, ^file_path1}, 30
    end
  end

  @tag :tmp_dir
  @tag :capture_log
  test "smoke test", %{tmp_dir: tmp_dir} do
    opts = [
      backend: SyncSet.Watcher.Backend.FileSystem,
      subscriber: self(),
      sync_dir: tmp_dir,
      debounce_ms: 50
    ]

    watcher_pid = start_supervised!({SyncSet.Watcher, opts})
    ref = Process.monitor(watcher_pid)
    # Sleep so we can wait for OS events to start arriving
    Process.sleep(200)
    # Create a file
    file_path = Path.join(tmp_dir, "file.txt")
    File.write!(file_path, "asdf")
    # Assert that the file is declared stable after a generous timeout
    # accounting for the OS filesystem produce the event consumed by the
    # watcher.
    assert_receive {:stable, ^file_path}, 3000

    # Obtain worker pid and kill worker process
    %{worker_pid: worker_pid} = :sys.get_state(watcher_pid)
    Process.exit(worker_pid, :kill)
    # FileSystem.start_link is called from inside the Watcher's init when the
    # FileSystem backend is used. Because the Watcher is a supervised child
    # in the test environment, the supervisor restarts it.
    # After the backend dies, a fresh watch should be established.
    assert_receive {:DOWN, ^ref, :process, ^watcher_pid, _reason}
  end
end
