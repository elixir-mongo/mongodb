defmodule Mongo do
  @moduledoc """
  TODO
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
    runCommand(pool, query, opts)["n"]
    |> trunc
  end

  @spec distinct(Pool.t, collection, String.t | atom, BSON.document, Keyword.t) :: [BSON.t]
  def distinct(pool, coll, field, filter, opts \\ []) do
    query = [
      distinct: coll,
      key: field,
      query: filter,
      maxTimeMS: opts[:max_time]
    ] |> filter_nils

    opts = Keyword.drop(opts, ~w(max_time))

    runCommand(pool, query, opts)["values"]
  end

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

    drop = ~w(comment max_time modifiers sort cursor_type projection skip cursor_timeout)a
    opts = cursor_type(opts[:cursor_type]) ++ Keyword.drop(opts, drop)

    unless Keyword.get(opts, :cursor_timeout, true) do
      opts = [{:no_cursor_timeout, true} | opts]
    end

    cursor(pool, coll, query, select, opts)
  end

  @spec runCommand(Pool.t, BSON.document, Keyword.t) :: BSON.document
  def runCommand(pool, query, opts \\ []) do
    result =
      pool.run(fn pid ->
        Connection.find_one(pid, "$cmd", query, [], opts)
      end)

    case result do
      %{"ok" => 1.0} = doc ->
        doc
      %{"ok" => 0.0, "errmsg" => reason, "code" => code} ->
        raise %Mongo.Error{message: "runCommand failed: #{reason}", code: code}
    end
  end

  @spec insert_one(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.InsertOneResult.t}
  def insert_one(pool, coll, doc, opts \\ []) do
    single_doc(doc)

    pool.run(fn pid ->
      case Connection.insert(pid, coll, doc, opts) do
        :ok ->
          :ok
        {:ok, %WriteResult{inserted_ids: ids}} ->
          {:ok, %Mongo.InsertOneResult{inserted_id: List.first(ids)}}
        {:error, error} ->
          raise error
      end
    end)
  end

  @spec insert_many(Pool.t, collection, [BSON.document], Keyword.t) :: :ok | {:ok, Mongo.InsertManyResult.t}
  def insert_many(pool, coll, docs, opts \\ []) do
    many_docs(docs)

    # NOTE: Only for 2.4
    ordered? = Keyword.get(opts, :ordered, true)
    opts = [continue_on_error: not ordered?] ++ opts

    pool.run(fn pid ->
      case Connection.insert(pid, coll, docs, opts) do
        :ok ->
          :ok
        {:ok, %WriteResult{inserted_ids: ids}} ->
          ids = Enum.with_index(ids)
                |> Enum.into(%{}, fn {x, y} -> {y, x} end)
          {:ok, %Mongo.InsertManyResult{inserted_ids: ids}}
        {:error, error} ->
          raise error
      end
    end)
  end

  @spec delete_one(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.DeleteResult.t}
  def delete_one(pool, coll, filter, opts \\ []) do
    opts = [multi: false] ++ opts

    pool.run(fn pid ->
      case Connection.remove(pid, coll, filter, opts) do
        :ok ->
          :ok
        {:ok, %WriteResult{num_matched: n, num_removed: n}} ->
          {:ok, %Mongo.DeleteResult{deleted_count: n}}
        {:error, error} ->
          raise error
      end
    end)
  end

  @spec delete_many(Pool.t, collection, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.DeleteResult.t}
  def delete_many(pool, coll, filter, opts \\ []) do
    opts = [multi: true] ++ opts

    pool.run(fn pid ->
      case Connection.remove(pid, coll, filter, opts) do
        :ok ->
          :ok
        {:ok, %WriteResult{num_matched: n, num_removed: n}} ->
          {:ok, %Mongo.DeleteResult{deleted_count: n}}
        {:error, error} ->
          raise error
      end
    end)
  end

  @spec replace_one(Pool.t, collection, BSON.document, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.UpdateResult.t}
  def replace_one(pool, coll, filter, replacement, opts \\ []) do
    modifier_docs(replacement, :replace)
    opts = [multi: false] ++ opts

    pool.run(fn pid ->
      case Connection.update(pid, coll, filter, replacement, opts) do
        :ok ->
          :ok
        {:ok, %WriteResult{num_matched: matched, num_modified: modified, upserted_id: id}} ->
          {:ok, %Mongo.UpdateResult{matched_count: matched, modified_count: modified, upserted_id: id}}
        {:error, error} ->
          raise error
      end
    end)
  end

  @spec update_one(Pool.t, collection, BSON.document, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.UpdateResult.t}
  def update_one(pool, coll, filter, update, opts \\ []) do
    modifier_docs(update, :update)
    opts = [multi: false] ++ opts

    pool.run(fn pid ->
      case Connection.update(pid, coll, filter, update, opts) do
        :ok ->
          :ok
        {:ok, %WriteResult{num_matched: matched, num_modified: modified, upserted_id: id}} ->
          {:ok, %Mongo.UpdateResult{matched_count: matched, modified_count: modified, upserted_id: id}}
        {:error, error} ->
          raise error
      end
    end)
  end

  @spec update_many(Pool.t, collection, BSON.document, BSON.document, Keyword.t) :: :ok | {:ok, Mongo.UpdateResult.t}
  def update_many(pool, coll, filter, update, opts \\ []) do
    modifier_docs(update, :update)
    opts = [multi: true] ++ opts

    pool.run(fn pid ->
      case Connection.update(pid, coll, filter, update, opts) do
        :ok ->
          :ok
        {:ok, %WriteResult{num_matched: matched, num_modified: modified, upserted_id: id}} ->
          {:ok, %Mongo.UpdateResult{matched_count: matched, modified_count: modified, upserted_id: id}}
        {:error, error} ->
          raise error
      end
    end)
  end

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
          if replace.upserted_id do
            ids = Map.put(result.upserted_ids, ix, replace.upserted_id)
                  |> Enum.into(result.upserted_ids)
          else
            ids = result.upserted_ids
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

  defp many_docs([first | _]) when not is_tuple(first), do: :ok
end
