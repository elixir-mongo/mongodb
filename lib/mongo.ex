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

  def find(conn, coll, query, select, opts \\ []) do
    GenServer.call(conn, {:find, coll, query, select, opts})
  end

  def get_more(conn, coll, cursor_id, opts \\ []) do
    GenServer.call(conn, {:get_more, coll, cursor_id, opts})
  end

  def kill_cursors(conn, cursor_ids) do
    GenServer.call(conn, {:kill_cursors, List.wrap(cursor_ids)})
  end

  def find_one(conn, coll, query, select, opts \\ []) do
    GenServer.call(conn, {:find_one, coll, query, select, opts})
  end

  def insert(conn, coll, docs, opts \\ []) do
    docs = assign_ids(docs)
    GenServer.call(conn, {:insert, coll, docs, opts})
  end

  def update(conn, coll, query, update, opts \\ []) do
    GenServer.call(conn, {:update, coll, query, update, opts})
  end

  def remove(conn, coll, query, opts \\ []) do
    GenServer.call(conn, {:remove , coll, query, opts})
  end

  defp assign_ids(doc) when is_map(doc) do
    assign_id(doc)
  end

  defp assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
  end

  defp assign_id(%{_id: value} = map) when value != nil,
    do: map
  defp assign_id(%{"_id" => value} = map) when value != nil,
    do: map

  defp assign_id(map) when is_map(map) do
    list = Map.to_list(map)
    id   = Mongo.IdServer.new

    case list do
      [{key, _}|_] when is_atom(key) ->
        %BSON.Keyword{list: [{:_id, id}|list]}

      [{key, _}|_] when is_binary(key) ->
        %BSON.Keyword{list: [{"_id", id}|list]}

      [] ->
        # Why are you inserting empty documents =(
        %{"_id" => id}
    end
  end
end
