defmodule Mongo.InsertOneResult do
  @moduledoc """
  TODO
  """
  defstruct [:inserted_id]
end

defmodule Mongo.InsertManyResult do
  @moduledoc """
  TODO
  """
  defstruct [:inserted_ids]
end

defmodule Mongo.DeleteResult do
  @moduledoc """
  TODO
  """
  defstruct [:deleted_count]
end

defmodule Mongo.UpdateResult do
  @moduledoc """
  TODO
  """
  defstruct [:matched_count, :modified_count, :upserted_id]
end

defmodule Mongo.SaveOneResult do
  @moduledoc """
  TODO
  """
  defstruct [:matched_count, :modified_count, :upserted_id]
end

defmodule Mongo.SaveManyResult do
  @moduledoc """
  TODO
  """
  defstruct [:matched_count, :modified_count, :upserted_ids]
end

defmodule Mongo.ReadResult do
  @moduledoc false

  defstruct [
    :from,
    :num,
    :docs,
    :cursor_id
  ]
end

defmodule Mongo.WriteResult do
  @moduledoc false

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
