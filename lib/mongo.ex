defmodule Mongo do
  @moduledoc """
  The main entry point for doing queries. All functions take a pool to
  run the query on.

  ## Read options

  All read operations that returns a cursor take the following options
  for controlling the behaviour of the cursor.

    * `:batch_size` - Number of documents to fetch in each batch
    * `:limit` - Maximum number of documents to fetch with the cursor

  ## Write options

  All write operations take the following options for controlling the
  write concern.

    * `:w` - The number of servers to replicate to before returning from write
      operators, a 0 value will return immediately, :majority will wait until
      the operation propagates to a majority of members in the replica set
      (Default: 1)
    * `:j` If true, the write operation will only return after it has been
      committed to journal - (Default: false)
    * `:wtimeout` - If the write concern is not satisfied in the specified
      interval, the operation returns an error

  ## Logging

  All operations take a boolean `log` option, that determines, whether the
  pool's `log/5` function will be called.
  """

  alias Mongo.Connection
  alias Mongo.WriteResult
  alias Mongo.Pool

  @type collection :: String.t
  @opaque cursor :: Mongo.Cursor.t | Mongo.AggregationCursor.t | Mongo.SinglyCursor.t

  @doc false
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Mongo.IdServer, []),
      worker(Mongo.PBKDF2Cache, [])
    ]

    opts = [strategy: :one_for_one, name: Mongo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Performs aggregation operation using the aggregation pipeline.

  ## Options

    * `:allow_disk_use` - Enables writing to temporary files (Default: false)
    * `:max_time` - Specifies a time limit in milliseconds
    * `:use_cursor` - Use a cursor for a batched response (Default: true)
  """
  @spec aggregate(Pool.t, collection, [BSON.document], Keyword.t) :: cursor
  def aggregate(pool, coll, pipeline, opts \\ []) do
    query = [
      aggregate: coll,
      pipeline: pipeline,
      allowDiskUse: opts[:allow_disk_use],
      maxTimeMS: opts[:max_time]
    ] |> filter_nils

    cursor? = pool.version >= 1 and Keyword.get(opts, :use_cursor, true)

    if cursor? do
      cursor = %{
        batchSize: opts[:batch_size]
      }
      query = query ++ [cursor: filter_nils(cursor)]
    end

    opts = Keyword.drop(opts, ~w(allow_disk_use max_time use_cursor)a)

    if cursor? do
      aggregation_cursor(pool, "$cmd", query, nil, opts)
    else
      singly_cursor(pool, "$cmd", query, nil, opts)
    end
  end

  @doc """
  Returns the count of documents that would match a `find/4` query.

  ## Options

    * `:limit` - Maximum number of documents to fetch with the cursor
    * `:skip` - Number of documents to skip before returning the first
    * `:hint` - Hint which index to use for the query
  """
  @spec count(Pool.t, collection, BSON.document, Keyword.t) :: non_neg_integer
  def count(pool, coll, filter, opts \\ []) do
    query = [
      count: coll,
      query: filter,
      limit: opts[:limit],
      skip: opts[:skip],
      hint: opts[:hint]
    ] |> filter_nils

    opts = Keyword.drop(opts, ~w(limit skip hint)a)

    # Mongo 2.4 and 2.6 returns a float
    run_command(pool, query, opts)["n"]
    |> trunc
  end

  @doc """
  Finds the distinct values for a specified field across a collection.

  ## Options

    * `:max_time` - Specifies a time limit in milliseconds
  """
  @spec distinct(Pool.t, collection, String.t | atom, BSON.document, Keyword.t) :: [BSON.t]
  def distinct(pool, coll, field, filter, opts \\ []) do
    query = [
      distinct: coll,
      key: field,
      query: filter,
      maxTimeMS: opts[:max_time]
    ] |> filter_nils

    opts = Keyword.drop(opts, ~w(max_time))

    run_command(pool, query, opts)["values"]
  end

  @doc """
  Selects documents in a collection and returns a cursor for the selected
  documents.

  ## Options

    * `:comment` - Associates a comment to a query
    * `:cursor_type` - Set to :tailable or :tailable_await to return a tailable
      cursor
    * `:max_time` - Specifies a time limit in milliseconds
    * `:modifiers` - Meta-operators modifying the output or behavior of a query,
      see http://docs.mongodb.org/manual/reference/operator/query-modifier/
    * `:cursor_timeout` - Set to false if cursor should not close after 10
      minutes (Default: true)
    * `:order_by` - Sorts the results of a query in ascending or descending order
    * `:projection` - Limits the fields to return for all matching document
    * `:skip` - The number of documents to skip before returning (Default: 0)
  """
  @spec find(Pool.t, collection, BSON.document, Keyword.t) :: cursor
  def find(pool, coll, filter, opts \\ []) do
    query = [
      {"$comment", opts[:comment]},
      {"$maxTimeMS", opts[:max_time]},
      {"$orderby", opts[:sort]}
    ] ++ Enum.into(opts[:modifiers] || [], [])

    query = filter_nils(query)

    if query == [] do
      query = filter
    else
      filter = normalize_doc(filter)
      unless List.keymember?(filter, "$query", 0) do
        filter = [{"$query", filter}]
      end
      query = filter ++ query
    end

    select = opts[:projection]

    unless Keyword.get(opts, :cursor_timeout, true) do
      opts = [{:no_cursor_timeout, true} | opts]
    end

    drop = ~w(comment max_time modifiers sort cursor_type projection cursor_timeout)a
    opts = cursor_type(opts[:cursor_type]) ++ Keyword.drop(opts, drop)

    cursor(pool, coll, query, select, opts)
  end

  @doc """
  Issue a database command. If the command has parameters use a keyword
  list for the document because the "command key" has to be the first
  in the document.
  """
  @spec run_command(Pool.t, BSON.document, Keyword.t) :: BSON.document
  def run_command(pool, query, opts \\ []) do
    result =
      Pool.run_with_log(pool, :run_command, [query], opts, fn pid ->
        case Connection.find_one(pid, "$cmd", query, [], opts) do
          %{"ok" => 1.0} = doc ->
            {:ok, doc}
          %{"ok" => 0.0, "errmsg" => reason} = error ->
            {:error, %Mongo.Error{message: "run_command failed: #{reason}", code: error["code"]}}
        end
      end)

    case result do
      {:ok, doc}      -> doc
      {:error, error} -> raise error
    end
  end

  @doc """
  Insert a single document into the collection.

  If the document is missing the `_id` field or it is `nil`, an ObjectId
  will be generated, inserted into the document, and returned in the result struct.
  """
  @spec insert_one(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.InsertOneResult.t}
  def insert_one(pool, coll, doc, opts \\ []) do
    single_doc(doc)
    result =
      Pool.run_with_log(pool, :insert_one, [coll, doc], opts, fn pid ->
        Connection.insert(pid, coll, doc, opts)
      end)

    case result do
      :ok ->
        :ok
      {:ok, %WriteResult{inserted_ids: ids}} ->
        {:ok, %Mongo.InsertOneResult{inserted_id: List.first(ids)}}
      {:error, error} ->
        raise error
    end
  end

  @doc """
  Insert multiple documents into the collection.

  If any of the documents is missing the `_id` field or it is `nil`, an ObjectId
  will be generated, and insertd into the document.
  Ids of all documents will be returned in the result struct.

  ## Options

    * `:continue_on_error` - even if insert fails for one of the documents
      continue inserting the remaining ones (default: `false`)
  """
  # TODO describe the ordered option
  @spec insert_many(Pool.t, collection, [BSON.document], Keyword.t) :: :ok | {:ok, Mongo.InsertManyResult.t}
  def insert_many(pool, coll, docs, opts \\ []) do
    many_docs(docs)

    # NOTE: Only for 2.4
    ordered? = Keyword.get(opts, :ordered, true)
    dbopts = [continue_on_error: not ordered?] ++ opts

    result =
      Pool.run_with_log(pool, :insert_many, [coll, docs], opts, fn pid ->
        Connection.insert(pid, coll, docs, dbopts)
      end)

    case result do
      :ok ->
        :ok
      {:ok, %WriteResult{inserted_ids: ids}} ->
        ids = Enum.with_index(ids)
              |> Enum.into(%{}, fn {x, y} -> {y, x} end)
      {:ok, %Mongo.InsertManyResult{inserted_ids: ids}}
      {:error, error} ->
        raise error
    end
  end

  @doc """
  Remove a document matching the filter from the collection.
  """
  @spec delete_one(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.DeleteResult.t}
  def delete_one(pool, coll, filter, opts \\ []) do
    dbopts = [multi: false] ++ opts

    result =
      Pool.run_with_log(pool, :delete_one, [coll, filter], opts, fn pid ->
        Connection.remove(pid, coll, filter, dbopts)
      end)

    case result do
      :ok ->
        :ok
      {:ok, %WriteResult{num_matched: n, num_removed: n}} ->
        {:ok, %Mongo.DeleteResult{deleted_count: n}}
      {:error, error} ->
        raise error
    end
  end

  @doc """
  Remove all documents matching the filter from the collection.
  """
  @spec delete_many(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.DeleteResult.t}
  def delete_many(pool, coll, filter, opts \\ []) do
    dbopts = [multi: true] ++ opts

    result =
      Pool.run_with_log(pool, :delete_many, [coll, filter], opts, fn pid ->
        Connection.remove(pid, coll, filter, dbopts)
      end)

    case result do
      :ok ->
        :ok
      {:ok, %WriteResult{num_matched: n, num_removed: n}} ->
        {:ok, %Mongo.DeleteResult{deleted_count: n}}
      {:error, error} ->
        raise error
    end
  end

  @doc """
  Replace a single document matching the filter with the new document.

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec replace_one(Pool.t, collection, BSON.document, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.UpdateResult.t}
  def replace_one(pool, coll, filter, replacement, opts \\ []) do
    modifier_docs(replacement, :replace)
    dbopts = [multi: false] ++ opts

    result =
      Pool.run_with_log(pool, :replace_one, [coll, filter, replacement], opts, fn pid ->
        Connection.update(pid, coll, filter, replacement, dbopts)
      end)

    case result do
      :ok ->
        :ok
      {:ok, %WriteResult{num_matched: matched, num_modified: modified, upserted_id: id}} ->
        {:ok, %Mongo.UpdateResult{matched_count: matched, modified_count: modified, upserted_id: id}}
      {:error, error} ->
        raise error
    end
  end

  @doc """
  Update a single document matching the filter.

  Uses MongoDB update operators to specify the updates. For more information
  please refer to the
  [MongoDB documentation](http://docs.mongodb.org/manual/reference/operator/update/)
  
  Example:

      Mongo.update_one(MongoPool,
        "my_test_collection",
        %{"filter_field": "filter_value"},
        %{"$set": %{"modified_field": "new_value"}})

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec update_one(Pool.t, collection, BSON.document, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.UpdateResult.t}
  def update_one(pool, coll, filter, update, opts \\ []) do
    modifier_docs(update, :update)
    dbopts = [multi: false] ++ opts

    result =
      Pool.run_with_log(pool, :update_one, [coll, filter, update], opts, fn pid ->
        Connection.update(pid, coll, filter, update, dbopts)
      end)

    case result do
      :ok ->
        :ok
      {:ok, %WriteResult{num_matched: matched, num_modified: modified, upserted_id: id}} ->
        {:ok, %Mongo.UpdateResult{matched_count: matched, modified_count: modified, upserted_id: id}}
      {:error, error} ->
        raise error
    end
  end

  @doc """
  Update all documents matching the filter.

  Uses MongoDB update operators to specify the updates. For more information
  please refer to the
  [MongoDB documentation](http://docs.mongodb.org/manual/reference/operator/update/)

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec update_many(Pool.t, collection, BSON.document, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.UpdateResult.t}
  def update_many(pool, coll, filter, update, opts \\ []) do
    modifier_docs(update, :update)
    dbopts = [multi: true] ++ opts

    result =
      Pool.run_with_log(pool, :update_many, [coll, filter, update], opts, fn pid ->
        Connection.update(pid, coll, filter, update, dbopts)
      end)

    case result do
      :ok ->
        :ok
      {:ok, %WriteResult{num_matched: matched, num_modified: modified, upserted_id: id}} ->
        {:ok, %Mongo.UpdateResult{matched_count: matched, modified_count: modified, upserted_id: id}}
      {:error, error} ->
        raise error
    end
  end

  @doc """
  Updates an existing document or inserts a new one.

  If the document does not contain the `_id` field, then the `insert_one/3`
  function is used to persist the document, otherwise `replace_one/5` is used,
  where the filter is the `_id` field, and the `:upsert` option is set to `true`.
  """
  @spec save_one(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.SaveOneResult.t}
  def save_one(pool, coll, doc, opts \\ []) do
    case get_id(doc) do
      {:ok, id} ->
        opts = [upsert: true] ++ opts
        case replace_one(pool, coll, %{_id: id}, doc, opts) do
          :ok ->
            :ok
          {:ok, result} ->
            %Mongo.SaveOneResult{
              matched_count: result.matched_count,
              modified_count: result.modified_count,
              upserted_id: result.upserted_id}
        end
      :error ->
        case insert_one(pool, coll, doc, opts) do
          :ok ->
            :ok
          {:ok, result} ->
            %Mongo.SaveOneResult{
              matched_count: 0,
              modified_count: 0,
              upserted_id: result.inserted_id}
        end
    end
    |> save_result
  end

  @doc """
  Updates documents or inserts them.

  For the documents that does not contain the `_id` field, `insert_many/3`
  function is used to persist them, for those that do contain the `_id` field,
  the `replace_one/5` function is invoked for each document separately, where
  the filter is the `_id` field, and the `:upsert` option is set to `true`.

  ## Options

    * `:ordered` - if set to `false` will group all documents to be inserted
      together, otherwise it will preserve the order, but it may be slow
      for large number of documents (default: `false`)
  """
  @spec save_many(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.SaveManyResult.t}
  def save_many(pool, coll, docs, opts \\ []) do
    many_docs(docs)

    # NOTE: Only for 2.4
    ordered? = Keyword.get(opts, :ordered, true)
    opts = [continue_on_error: not ordered?, upsert: true] ++ opts
    docs = docs_id_ix(docs)

    if ordered? do
      # Ugh, horribly inefficient
      save_ordered(pool, coll, docs, opts)
    else
      save_unordered(pool, coll, docs, opts)
    end
    |> save_result
  end

  defp save_result(:ok),
    do: :ok
  defp save_result(result),
    do: {:ok, result}

  defp save_ordered(pool, coll, docs, opts) do
    chunked_docs = Enum.chunk_by(docs, fn {_, id, _} -> id == :error end)
    result = %Mongo.SaveManyResult{matched_count: 0, modified_count: 0, upserted_ids: %{}}

    Enum.reduce(chunked_docs, result, fn docs, result ->
      {ix, id, _doc} = hd(docs)
      if id == :error do
        save_insert(result, ix, pool, coll, docs, opts)
      else
        save_replace(result, ix, pool, coll, docs, opts)
      end
    end)
  end

  defp save_unordered(pool, coll, docs, opts) do
    docs = Enum.group_by(docs, fn {_, id, _} -> id == :error end)
    insert_docs  = docs[true] || []
    replace_docs = docs[false] || []

    %Mongo.SaveManyResult{matched_count: 0, modified_count: 0, upserted_ids: %{}}
    |> save_insert(0, pool, coll, insert_docs, opts)
    |> save_replace(length(insert_docs), pool, coll, replace_docs, opts)
  end

  defp save_insert(result, _ix, _pool, _coll, [], _opts) do
    result
  end

  defp save_insert(result, ix, pool, coll, docs, opts) do
    docs = Enum.map(docs, &elem(&1, 2))

    case insert_many(pool, coll, docs, opts) do
      :ok ->
        :ok
      {:ok, insert} ->
        ids = list_ix(insert.inserted_ids, ix)
              |> Enum.into(result.upserted_ids)
        %{result | upserted_ids: ids}
    end
  end

  defp save_replace(result, ix, pool, coll, docs, opts) do
    Enum.reduce(docs, {ix, result}, fn {_ix, {:ok, id}, doc}, {ix, result} ->
      case replace_one(pool, coll, %{_id: id}, doc, opts) do
        :ok ->
          {0, :ok}
        {:ok, replace} ->
          ids =
            if replace.upserted_id do
              Map.put(result.upserted_ids, ix, replace.upserted_id)
              |> Enum.into(result.upserted_ids)
            else
              result.upserted_ids
            end

          result =
            %{result | matched_count: result.matched_count + replace.matched_count,
                       modified_count: result.modified_count + replace.modified_count,
                       upserted_ids: ids}
          {ix+1, result}
      end
    end)
    |> elem(1)
  end

  defp list_ix(enum, offset) do
    Enum.map(enum, fn {ix, elem} ->
      {ix+offset, elem}
    end)
  end

  defp docs_id_ix(docs) do
    Enum.reduce(docs, {0, []}, fn doc, {ix, docs} ->
      {ix+1, [{ix, get_id(doc), doc} | docs]}
    end)
    |> elem(1)
    |> Enum.reverse
  end

  defp modifier_docs([{key, _}|_], type),
    do: key |> key_to_string |> modifier_key(type)
  defp modifier_docs(map, _type) when is_map(map) and map_size(map) == 0,
    do: :ok
  defp modifier_docs(map, type) when is_map(map),
    do: Enum.at(map, 0) |> elem(0) |> key_to_string |> modifier_key(type)
  defp modifier_docs(list, type) when is_list(list),
    do: Enum.map(list, &modifier_docs(&1, type))

  defp modifier_key(<<?$, _::binary>>, :replace),
    do: raise(ArgumentError, "replace does not allow atomic modifiers")
  defp modifier_key(<<?$, _::binary>>, :update),
    do: :ok
  defp modifier_key(<<_, _::binary>>, :update),
    do: raise(ArgumentError, "update only allows atomic modifiers")
  defp modifier_key(_, _),
    do: :ok

  defp key_to_string(key) when is_atom(key),
    do: Atom.to_string(key)
  defp key_to_string(key) when is_binary(key),
    do: key

  defp cursor(pool, coll, query, select, opts) do
    %Mongo.Cursor{
      pool: pool,
      coll: coll,
      query: query,
      select: select,
      opts: opts}
  end

  defp singly_cursor(pool, coll, query, select, opts) do
    %Mongo.SinglyCursor{
      pool: pool,
      coll: coll,
      query: query,
      select: select,
      opts: opts}
  end

  defp aggregation_cursor(pool, coll, query, select, opts) do
    %Mongo.AggregationCursor{
      pool: pool,
      coll: coll,
      query: query,
      select: select,
      opts: opts}
  end

  defp filter_nils(keyword) when is_list(keyword) do
    Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
  end

  defp filter_nils(map) when is_map(map) do
    Enum.reject(map, fn {_key, value} -> is_nil(value) end)
    |> Enum.into(%{})
  end

  defp normalize_doc(doc) do
    Enum.reduce(doc, {:unknown, []}, fn
      {key, _value}, {:binary, _acc} when is_atom(key) ->
        invalid_doc(doc)

      {key, _value}, {:atom, _acc} when is_binary(key) ->
        invalid_doc(doc)

      {key, value}, {_, acc} when is_atom(key) ->
        {:atom, [{key, value}|acc]}

      {key, value}, {_, acc} when is_binary(key) ->
        {:binary, [{key, value}|acc]}
    end)
    |> elem(1)
    |> Enum.reverse
  end

  defp invalid_doc(doc) do
    message = "invalid document containing atom and string keys: #{inspect doc}"
    raise ArgumentError, message
  end

  defp cursor_type(nil),
    do: []
  defp cursor_type(:tailable),
    do: [tailable_cursor: true]
  defp cursor_type(:tailable_await),
    do: [tailable_cursor: true, await_data: true]

  defp get_id(doc) do
    case fetch_value(doc, "_id") do
      {:ok, id}  -> {:ok, id}
      :error     -> fetch_value(doc, :_id)
    end
  end

  defp fetch_value(doc, key) do
    case Dict.fetch(doc, key) do
      {:ok, nil} -> :error
      {:ok, id}  -> {:ok, id}
      :error     -> :error
    end
  end

  defp single_doc(doc) when is_map(doc), do: :ok
  defp single_doc([]), do: :ok
  defp single_doc([{_, _} | _]), do: :ok
  defp single_doc(other) do
    raise ArgumentError, "expected single document, got: #{inspect other}"
  end

  defp many_docs([first | _]) when not is_tuple(first), do: :ok
  defp many_docs(other) do
    raise ArgumentError, "expected list of documents, got: #{inspect other}"
  end
end
