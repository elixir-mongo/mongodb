defmodule Mongo.ReadResult do
  defstruct [
    :from,
    :num,
    :docs,
    :cursor_id
  ]
end

defmodule Mongo.InsertOneResult do
  defstruct [:inserted_id]
end

defmodule Mongo.InsertManyResult do
  defstruct [:inserted_ids]
end

defmodule Mongo.WriteResult do
  # On 2.4 num_modified will always be nil

  defstruct [
    :type,
    :num_inserted,
    :num_matched,
    :num_modified,
    :num_removed,
    :upserted_id,
    :inserted_ids
  ]
end
