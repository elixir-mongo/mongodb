defmodule Mongo.SessionPool do
  use GenServer

  @name __MODULE__

  defstruct [sessions: [], monitors: %{}]

  def start_link(opts) do
    name = Keyword.get(opts, :name, @name)

    GenServer.start_link(__MODULE__, [], name: name)
  end

  def checkout(pool \\ @name, conn) do
    GenServer.call(pool, {:checkout, self(), conn})
  end

  def checkin(pool \\ @name, session) do
    GenServer.call(pool, {:checkin, self(), session})
  end

  @impl GenServer
  def init(_) do
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_call({:checkout, pid, conn}, _from, %__MODULE__{sessions: sessions, monitors: monitors}) do
    {session, rest} = do_checkout(sessions, conn)

    ref = Process.monitor(pid)
    monitors = Map.put(monitors, ref, session)

    {:reply, {:ok, session}, %__MODULE__{sessions: rest, monitors: monitors}}
  end

  def handle_call({:checkin, pid, session}, _from, %__MODULE__{sessions: sessions, monitors: monitors}) do
    sessions = [session | sessions]
    monitors = for {ref, p} <- monitors, p != pid, into: %{}, do: {ref, p}

    {:reply, :ok, %__MODULE__{sessions: sessions, monitors: monitors}}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {^pid, monitors} ->
        sessions =
          if Mongo.Session.ended?(pid) do
            state.sessions
          else
            [pid | state.sessions]
          end

        {:noreply, struct(state, sessions: sessions, monitors: monitors)}

      {nil, _monitors} ->
        sessions = Enum.reject(state.sessions, & &1 == pid)

        {:noreply, struct(state, sessions: sessions)}
    end
  end

  defp do_checkout([], conn) do
    session = Mongo.start_session(conn)

    {session, []}
  end

  defp do_checkout([session | rest], _conn), do: {session, rest}
end
