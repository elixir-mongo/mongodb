defmodule Mongo do
  alias Mongo.Connection
  alias Mongo.SinglyCursor
  alias Mongo.AggregationCursor

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

    opts = Keyword.drop(opts, ~w(limit skip hint))

    runCommand(pool, query, opts)["n"]
  end

  def runCommand(pool, query, opts \\ []) do
    result =
      pool.transaction(fn pid ->
        Connection.find_one(pid, "$cmd", query, [], [num_return: -1] ++ opts)
      end)

    case result do
      %{"ok" => 1.0} = doc ->
        doc
      %{"ok" => 0.0, "errmsg" => reason, "code" => code} ->
        raise %Mongo.Error{message: "runCommand failed: #{reason}", code: code}
    end
  end

  defp singly_cursor(pool, coll, query, select, opts) do
    %SinglyCursor{
      pool: pool,
      coll: coll,
      query: query,
      select: select,
      opts: opts}
  end

  defp aggregation_cursor(pool, coll, query, select, opts) do
    %AggregationCursor{
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
end
