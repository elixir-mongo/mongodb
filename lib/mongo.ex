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

  defp assign_ids([{_, _} | _] = doc) do
    assign_id(doc)
  end

  defp assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
  end

  defp assign_id(%{_id: value} = map) when value != nil,
    do: map
  defp assign_id(%{"_id" => value} = map) when value != nil,
    do: map

  defp assign_id([{_, _} | _] = keyword) do
    case Keyword.take(keyword, [:_id, "_id"]) do
      [{_key, value} | _] when value != nil ->
        keyword
      [] ->
        add_id(keyword)
    end
  end

  defp assign_id(map) when is_map(map) do
    map |> Map.to_list |> add_id
  end

  defp add_id(doc) do
    add_id(doc, Mongo.IdServer.new)
  end
  defp add_id([{key, _}|_] = list, id) when is_atom(key) do
    [{:_id, id}|list]
  end
  defp add_id([{key, _}|_] = list, id) when is_binary(key) do
    [{"_id", id}|list]
  end
  defp add_id([], id) do
    # Why are you inserting empty documents =(
    %{"_id" => id}
  end
end
