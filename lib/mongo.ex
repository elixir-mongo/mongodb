defmodule Mongo do
  alias Mongo.Connection
  alias Mongo.WriteResult

  @doc false
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Mongo.IdServer, [])
    ]

    opts = [strategy: :one_for_one, name: Mongo.Supervisor]
    Supervisor.start_link(children, opts)
  end

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

  def find(pool, coll, filter, opts \\ []) do
    query = [
      "$comment": opts[:comment],
      "$maxTimeMS": opts[:max_time],
      "$orderby": opts[:sort]
    ] ++ Enum.into(opts[:modifiers] || [], [])

    query = filter_nils(query)

    if query == [] do
      query = filter
    else
      filter = normalize_doc(filter)
      unless Keyword.has_key?(filter, "$query") do
        filter = [{"$query", filter}]
      end
      query = filter ++ query
    end

    select = opts[:projection]

    opts = cursor_type(opts[:cursor_type]) ++
           Keyword.drop(opts, ~w(comment max_time modifiers sort cursor_type projection)a)

    cursor(pool, coll, query, select, opts)
  end

  def runCommand(pool, query, opts \\ []) do
    result =
      transaction(pool, fn pid ->
        Connection.find_one(pid, "$cmd", query, [], opts)
      end)

    case result do
      %{"ok" => 1.0} = doc ->
        doc
      %{"ok" => 0.0, "errmsg" => reason, "code" => code} ->
        raise %Mongo.Error{message: "runCommand failed: #{reason}", code: code}
    end
  end

  def insert_one(pool, coll, doc, opts \\ []) do
    single_doc(doc)

    transaction(pool, fn pid ->
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
    Enum.map(doc, fn
      {key, value} when is_binary(key) ->
        {key, value}
      {key, value} when is_atom(key) ->
        {Atom.to_string(key), value}
    end)
  end

  defp cursor_type(nil),
    do: []
  defp cursor_type(:tailable),
    do: [tailable_cursor: true]
  defp cursor_type(:tailable_await),
    do: [tailable_cursor: true, await_data: true]

  defp single_doc(doc) when is_map(doc), do: :ok
  defp single_doc([]), do: :ok
  defp single_doc([{_, _} | _]), do: :ok

  @doc false
  def transaction(pool, fun) when is_atom(pool),
    do: pool.transaction(fun)
  def transaction(conn, fun) when is_pid(conn),
    do: fun.(conn)
end
