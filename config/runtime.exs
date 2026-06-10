# Boot time config (every time the node starts). These values come from the machines's
# environment and are read via System.get_env. The config is evaluated between
# compilation and the system actually starting.
import Config

if sync_dir = System.get_env("SYNC_DIR") do
  config :sync_set, sync_dir: sync_dir
end

if port = System.get_env("DATA_PORT") do
  config :sync_set, data_port: String.to_integer(port)
end

if dets_path = System.get_env("DETS_PATH") do
  config :sync_set, dets_path: dets_path
end
