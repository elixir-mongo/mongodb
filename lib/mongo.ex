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
    {insert, return} = assign_ids(docs, [])

    case GenServer.call(conn, {:insert, coll, insert, opts}) do
      :ok ->
        {:ok, return}
      {:error, _} = error ->
        error
    end
  end

  defp assign_ids([doc|tail], {insert_acc, return_acc}) do
    {insert, return} = assign_id(doc)
    assign_ids(tail, {[insert|insert_acc], [return|return_acc]})
  end

  defp assign_ids([], {insert_acc, return_acc}) do
    {Enum.reverse(insert_acc), Enum.reverse(return_acc)}
  end

  defp assign_ids(map, []) when is_map(map) do
    assign_id(map)
  end

  defp assign_id(%{_id: value} = map) when value != nil,
    do: {map, map}
  defp assign_id(%{"_id" => value} = map) when value != nil,
    do: {map, map}

  defp assign_id(map) when is_map(map) do
    list = Map.to_list(map)
    id   = Mongo.IdServer.new

    case list do
      [{key, _}|_] when is_atom(key) ->
        keyword = %BSON.Keyword{list: [{:_id, id}|list]}
        map     = Map.put(map, :_id, id)

      [{key, _}|_] when is_binary(key) ->
        keyword = %BSON.Keyword{list: [{"_id", id}|list]}
        map     = Map.put(map, :_id, id)

      [] ->
        # Why are you inserting empty documents =(
        keyword = %{"_id" => id}
        map     = keyword
    end

    {keyword, map}
  end
end
