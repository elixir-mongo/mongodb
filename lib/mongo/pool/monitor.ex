defmodule Mongo.Pool.Monitor do
  use GenServer
  alias Mongo.Connection

  def start_link(name, opts) do
    GenServer.start_link(__MODULE__, [name, opts])
  end

  def init([name, opts]) do
    :ets.new(name, [:named_table, read_concurrency: true])
    {:ok, conn} = Connection.start_link([on_connect: self] ++ opts)
    {:ok, {name, conn}}
  end

  def handle_info({Connection, :on_connect, conn}, {name, conn}) do
    version = Connection.wire_version(conn)
    :ets.insert(name, {:wire_version, version})
    {:noreply, {name, conn}}
  end
end
