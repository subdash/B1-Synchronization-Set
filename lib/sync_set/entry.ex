defmodule SyncSet.Entry do
  defstruct vector: %{}, checksum: nil, size: 0, deleted: false

  @type t :: %__MODULE__{
          vector: map(),
          checksum: String.t() | nil,
          size: integer(),
          deleted: boolean()
        }
end
