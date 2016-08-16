defmodule Mongo.Monitor do
  @moduledoc false
  use GenServer

  @ets __MODULE__.ETS

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def add_conn(pid, pool, version) do
    GenServer.call(__MODULE__, {:add_conn, pid, pool, version})
  end

  def wire_version(conn) do
    [{^conn, version}] = :ets.lookup(@ets, conn)
    version
  end

  def init([]) do
    :ets.new(@ets, [:named_table, read_concurrency: true])
    {:ok, %{monitors: %{}}}
  end

  def handle_call({:add_conn, pid, pool, version}, _from, state) do
    :ets.insert(@ets, {pid, version})
    ref = Process.monitor(pid)
    state = put_in(state.monitors[ref], pid)

    state =
      if pool && :ets.lookup(@ets, pool) == [] do
        :ets.insert(@ets, {pool, version})
        ref = Process.monitor(pool)
        put_in(state.monitors[ref], pool)
      else
        state
      end

    {:reply, :ok, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    pid = state.monitors[ref]
    :ets.delete(@ets, pid)
    monitors = Map.delete(state.monitors, ref)

    {:noreply, %{state | monitors: monitors}}
  end
end
