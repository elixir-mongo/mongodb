defmodule Mongo.SessionPool do
  use GenServer

  @name __MODULE__

  defstruct sessions: [], monitors: %{}

  def start_link(opts) do
    name = Keyword.get(opts, :name, @name)

    GenServer.start_link(__MODULE__, [], name: name)
  end

  def checkout(pool \\ @name, conn, opts) do
    GenServer.call(pool, {:checkout, self(), conn, opts})
  end

  def checkin(pool \\ @name, session) do
    GenServer.call(pool, {:checkin, self(), session})
  end

  @impl GenServer
  def init(_) do
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_call({:checkout, pid, conn, opts}, _from, %__MODULE__{
        sessions: sessions,
        monitors: monitors
      }) do
    {:ok, session, rest} = do_checkout(sessions, conn, opts)

    ref = Process.monitor(pid)
    monitors = Map.put(monitors, ref, session)

    {:reply, {:ok, session}, %__MODULE__{sessions: rest, monitors: monitors}}
  end

  def handle_call({:checkin, pid, session}, _from, %__MODULE__{
        sessions: sessions,
        monitors: monitors
      }) do
    _ = Mongo.Session.abort_transaction(session)
    sessions = [session | sessions]
    monitors = for {ref, p} <- monitors, p != pid, into: %{}, do: {ref, p}

    {:reply, :ok, %__MODULE__{sessions: sessions, monitors: monitors}}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        sessions = Enum.reject(state.sessions, &(&1 == pid))

        {:noreply, struct(state, sessions: sessions)}

      {_, monitors} ->
        sessions =
          if Mongo.Session.ended?(pid) do
            state.sessions
          else
            [pid | state.sessions]
          end

        {:noreply, struct(state, sessions: sessions, monitors: monitors)}
    end
  end

  defp do_checkout([], topology, opts) do
    with {:ok, conn, _, _} <- Mongo.select_server(topology, :read, opts),
         {:ok, %{"id" => id}} <- Mongo.direct_command(conn, %{startSession: 1}, opts),
         {:ok, session} <- Mongo.Session.Supervisor.start_child(topology, id, opts) do
      Process.monitor(session)

      {:ok, session, []}
    end
  end

  defp do_checkout([session | rest], _conn, opts) do
    :ok = Mongo.Session.set_options(session, opts)

    {:ok, session, rest}
  end
end
