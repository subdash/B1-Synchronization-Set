defmodule SyncSet.Conflict do
  alias SyncSet.Entry

  def winner(%Entry{checksum: left_checksum}, %Entry{checksum: right_checksum}) do
    case left_checksum >= right_checksum do
      true -> :left
      false -> :right
    end
  end

  def conflict_path(path, loser_checksum) do
    short = String.slice(loser_checksum, 0..7)

    dir =
      case Path.dirname(path) do
        "." -> ""
        other -> other
      end

    extension = Path.extname(path)
    filename = Path.basename(path, extension)
    filename = "#{filename}.sync-conflict-#{short}#{extension}"

    Path.join(dir, filename)
  end
end
