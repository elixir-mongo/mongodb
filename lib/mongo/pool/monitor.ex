defmodule Mongo.Pool.Monitor do
  @moduledoc false
  use GenServer
  alias Mongo.Connection

  def start_link(name, ets, opts) do
    GenServer.start_link(__MODULE__, [ets, opts], name: name)
  end

  def version(name, ets, timeout) do
    case :ets.lookup(ets, :wire_version) do
      [] ->
        GenServer.call(name, :version, timeout)
      [{:wire_version, version}] ->
        version
    end
  end

  def init([ets, opts]) do
    :ets.new(ets, [:named_table, read_concurrency: true])
    {:ok, conn} = Connection.start_link([on_connect: self] ++ opts)
    state = %{ets: ets, conn: conn, waiting: []}
    {:ok, state}
  end

  def handle_call(:version, _from, %{ets: ets, waiting: nil} = state) do
    [{:wire_version, version}] = :ets.lookup(ets, :wire_version)
    {:reply, version, state}
  end

  def handle_call(:version, from, %{waiting: waiting} = state) do
    {:noreply, %{state | waiting: [from|waiting]}}
  end

  def handle_info({Connection, :on_connect, conn},
                  %{ets: ets, conn: conn, waiting: waiting} = state) do
    version = Connection.wire_version(conn)
    :ets.insert(ets, {:wire_version, version})
    Enum.each(waiting, &GenServer.reply(&1, version))
    {:noreply, %{state | waiting: nil}}
  end
end
