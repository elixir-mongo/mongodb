defmodule Mongo do
  # alias Mongo.Pool
  # alias Mongo.Connection
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

    opts = Keyword.drop(opts, [:allow_disk_use, :max_time, :use_cursor])

    if cursor? do
      aggregation_cursor(pool, "$cmd", query, nil, opts)
    else
      singly_cursor(pool, "$cmd", query, nil, opts)
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
