defmodule SyncSet.Conflict do
  alias SyncSet.Entry

  def winner(%Entry{checksum: left_checksum}, %Entry{checksum: right_checksum}) do
    if left_checksum >= right_checksum, do: :left, else: :right
  end

  def conflict_path(path, loser_checksum) do
    short = String.slice(loser_checksum, 0..7)
    extension = Path.extname(path)
    base = Path.basename(path, extension)
    conflict_name = "#{base}.sync-conflict-#{short}#{extension}"

    case Path.dirname(path) do
      "." -> conflict_name
      dir -> Path.join(dir, conflict_name)
    end
  end
end
