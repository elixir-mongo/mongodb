defmodule Mongo do
  # TODO: Timeout

  @doc false
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Mongo.IdServer, [])
    ]

    opts = [strategy: :one_for_one, name: Mongo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def auth(conn, database, username, password) do
    GenServer.call(conn, {:auth, database, username, password})
  end

  def database(conn) do
    GenServer.call(conn, :database)
  end

  def database(conn, database) do
    GenServer.call(conn, {:database, database})
  end

  def find_one(conn, coll, query, select) do
    GenServer.call(conn, {:find_one, coll, query, select})
  end

  def insert(conn, coll, docs) do
    docs = assign_ids(docs)
    GenServer.call(conn, {:insert, coll, docs})
  end

  defp assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
  end

  defp assign_ids(map) when is_map(map) do
    assign_id(map)
  end

  defp assign_id(map) when is_map(map) do
    case Map.fetch(map, :_id) do
      {:ok, nil} ->
        Map.put(map, :_id, Mongo.IdServer.new)
      :error ->
        Map.put(map, :_id, Mongo.IdServer.new)
      {:ok, _} ->
        map
    end
  end
end
