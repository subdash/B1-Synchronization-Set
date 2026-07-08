import Config

# In :test, each test starts only the isolated subtree it needs (with injected deps
# and unique ports/paths), so we skip the global runtime supervision tree that
# start/2 would otherwise boot. See SyncSet.Application.runtime_children/0.
if config_env() == :test do
  config :sync_set, start_runtime?: false
end
