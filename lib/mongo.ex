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
end
