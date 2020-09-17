defmodule Mongo.SessionPool do
  use GenServer

  require Logger

  @name __MODULE__
  @max_age :erlang.convert_time_unit(10 * 60, :seconds, :native)

  @enforce_keys [:sessions, :max_age]
  defstruct sessions: nil, max_age: nil

  def start_link(opts) do
    name = Keyword.get(opts, :name, @name)
    max_age = @max_age

    GenServer.start_link(__MODULE__, max_age, name: name)
  end

  def checkout(pool \\ @name, conn, opts) do
    with {:ok, session} <- GenServer.call(pool, :checkout) do
      Mongo.Session.Supervisor.start_child(conn, session, opts, self())
    end
  end

  def checkin(pool \\ @name, id, txn) do
    GenServer.call(pool, {:checkin, id, txn})
  end

  @impl GenServer
  def init(max_age) do
    {:ok,
     %__MODULE__{
       sessions: :queue.new(),
       max_age: max_age
     }}
  end

  @impl GenServer
  def handle_call(:checkout, _ref, %__MODULE__{
        sessions: sessions,
        max_age: max_age
      }) do
    now = :erlang.monotonic_time()
    filtered = :queue.filter(fn %{last_use: last_use} -> now - last_use < max_age end, sessions)

    {session, rest} =
      case :queue.out(filtered) do
        {:empty, queue} -> {new_session(), queue}
        {{:value, session}, queue} -> {session, queue}
      end

    {:reply, {:ok, session}, %__MODULE__{sessions: rest, max_age: max_age}}
  end

  def handle_call({:checkin, id, txn}, _ref, state) do
    session = %{
      id: id,
      txn: txn,
      last_use: :erlang.monotonic_time()
    }

    {:reply, :ok, struct(state, sessions: :queue.in(session, state.sessions))}
  end

  defp new_session do
    %{
      id: uuidv4(),
      txn: 0,
      last_use: :erlang.monotonic_time()
    }
  end

  @uuid_v4 4
  @variant10 2

  defp uuidv4 do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    uuid = <<u0::48, @uuid_v4::4, u1::12, @variant10::2, u2::62>>
    %BSON.Binary{binary: uuid, subtype: :uuid}
  end
end
