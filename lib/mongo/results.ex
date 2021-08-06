defmodule Mongo.InsertOneResult do
  @moduledoc """
  The successful result struct of `Mongo.insert_one/4`. Its fields are:

    * `:inserted_id` - The id of the inserted document
  """

  @type t :: %__MODULE__{
          acknowledged: boolean,
          inserted_id: nil | BSON.ObjectId.t()
        }

  defstruct acknowledged: true, inserted_id: nil
end

defmodule Mongo.InsertManyResult do
  @moduledoc """
  The successful result struct of `Mongo.insert_many/4`. Its fields are:

    * `:inserted_ids` - The ids of the inserted documents indexed by their order
  """

  @type t :: %__MODULE__{
          acknowledged: boolean,
          inserted_ids: %{non_neg_integer => BSON.ObjectId.t()}
        }

  defstruct acknowledged: true, inserted_ids: nil
end

defmodule Mongo.DeleteResult do
  @moduledoc """
  The successful result struct of `Mongo.delete_one/4` and `Mongo.delete_many/4`.
  Its fields are:

    * `:deleted_count` - Number of deleted documents
  """

  @type t :: %__MODULE__{
          acknowledged: boolean,
          deleted_count: non_neg_integer
        }

  defstruct acknowledged: true, deleted_count: 0
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
          acknowledged: boolean,
          matched_count: non_neg_integer,
          modified_count: non_neg_integer,
          upserted_ids: nil | list(BSON.ObjectId.t())
        }

  defstruct acknowledged: true, matched_count: 0, modified_count: 0, upserted_ids: nil
end

defmodule Mongo.CreateIndexesResult do
  @moduledoc """
  The successful result struct of `Mongo.create_indexes/4`.

  Its fields are:

  * `:commit_quorum` - Quorum voting behaviour.  See https://docs.mongodb.com/manual/reference/command/createIndexes/#std-label-createIndexes-cmd-commitQuorum
  * `:created_collection_automatically` - `true` when the collection was implicitly created as part of the index creation command, `false` otherwise
  * `:num_indexes_after` - Number of indexes after the index creation command took place
  * `:num_indexes_before` - Number of indexes before the index creation command took place
  """

  @type t :: %__MODULE__{
          commit_quorum: non_neg_integer() | binary(),
          created_collection_automatically: boolean(),
          num_indexes_after: non_neg_integer(),
          num_indexes_before: non_neg_integer()
        }

  defstruct [
    :commit_quorum,
    :created_collection_automatically,
    :num_indexes_after,
    :num_indexes_before
  ]
end

defmodule Mongo.DropIndexResult do
  @moduledoc """
  The successful result struct of `Mongo.drop_index/4`.

  Its fields are:

  * `:num_indexes_was` - Number of indexes before the index was dropped.
  """

  @type t :: %__MODULE__{
    num_indexes_was: non_neg_integer()
  }

  defstruct [:num_indexes_was]
end
