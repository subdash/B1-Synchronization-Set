# Peer node sync dirs and DETS files live here. Each node removes its own on exit;
# this clears the shared base, plus anything a hard-killed run left behind.
File.rm_rf!(SyncSet.NodeHarness.tmp_base())
ExUnit.after_suite(fn _ -> File.rm_rf!(SyncSet.NodeHarness.tmp_base()) end)

ExUnit.start()
