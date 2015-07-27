defmodule Mongo.InsertOneResult do
  @moduledoc """
  The successful result struct of `Mongo.insert_one/4`. Its fields are:

    * `:inserted_id` - The id of the inserted document
  """

  @type t :: %__MODULE__{
    inserted_id: nil | BSON.ObjectId.t
  }

  defstruct [:inserted_id]
end

defmodule Mongo.InsertManyResult do
  @moduledoc """
  The successful result struct of `Mongo.insert_many/4`. Its fields are:

    * `:inserted_ids` - The ids of the inserted documents
  """

  @type t :: %__MODULE__{
    inserted_ids: [BSON.ObjectId.t]
  }

  defstruct [:inserted_ids]
end

defmodule Mongo.DeleteResult do
  @moduledoc """
  The successful result struct of `Mongo.delete_one/4` and `Mongo.delete_many/4`.
  Its fields are:

    * `:deleted_count` - Number of deleted documents
  """

  @type t :: %__MODULE__{
    deleted_count: non_neg_integer
  }

  defstruct [:deleted_count]
end

defmodule Mongo.UpdateResult do
  @moduledoc """
  The successful result struct of `Mongo.update_one/5`, `Mongo.update_many/5`
  and `Mongo.replace_one/5`. Its fields are:

    * `:matched_count` - Number of matched documents
    * `:modified_count` - Number of modified documents
    * `:upserted_id` - If the operation was an upsert, the upserted id
  """

  @type t :: %__MODULE__{
    matched_count: non_neg_integer,
    modified_count: non_neg_integer,
    upserted_id: nil | BSON.ObjectId.t
  }

  defstruct [:matched_count, :modified_count, :upserted_id]
end

defmodule Mongo.SaveOneResult do
  @moduledoc """
  The successful result struct of `Mongo.save_one/4`. Its fields are:

    * `:matched_count` - Number of matched documents
    * `:modified_count` - Number of modified documents
    * `:upserted_id` - If the operation was an upsert, the upserted id
  """

  @type t :: %__MODULE__{
    matched_count: non_neg_integer,
    modified_count: non_neg_integer,
    upserted_id: nil | BSON.ObjectId.t
  }

  defstruct [:matched_count, :modified_count, :upserted_id]
end

defmodule Mongo.SaveManyResult do
  @moduledoc """
  The successful result struct of `Mongo.save_many/4`. Its fields are:

    * `:matched_count` - Number of matched documents
    * `:modified_count` - Number of modified documents
    * `:upserted_ids` - If the operation was an upsert, the upserted ids
  """

  @type t :: %__MODULE__{
    matched_count: non_neg_integer,
    modified_count: non_neg_integer,
    upserted_ids: nil | BSON.ObjectId.t
  }

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
