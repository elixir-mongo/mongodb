defmodule Mongo.Session do
  @enforce_keys [:id, :pid, :ref]
  defstruct @enforce_keys ++ [
    :read_concern,
    :write_concern,
    :read_preference,
    casual_consistency: true,
    retry_writes: false,
    cluster_time: nil,
    txn: 0
  ]

  @opaque session :: pid()

  defmodule Supervisor do
    @moduledoc false

    def start_child(conn, id, opts) do
      DynamicSupervisor.start_child(__MODULE__, {Mongo.Session, {conn, id, opts, self()}})
    end

    def child_spec(_) do
      DynamicSupervisor.child_spec(strategy: :one_for_one, name: __MODULE__)
    end
  end

  @behaviour :gen_statem

  @spec start_transaction(session()) :: :ok
  @spec start_transaction(session(), keyword()) :: :ok
  def start_transaction(pid, opts \\ []) do
    :gen_statem.call(pid, {:start_transaction, opts})
  end

  @spec commit_transaction(session()) :: :ok | {:error, term}
  def commit_transaction(pid), do: :gen_statem.call(pid, :commit_transaction)

  @spec abort_transaction(session()) :: :ok
  def abort_transaction(pid), do: :gen_statem.call(pid, :abort_transaction)

  @spec end_session(session()) :: :ok
  def end_session(pid) do
    unless ended?(pid), do: :gen_statem.call(pid, :end_session)

    :ok
  end

  @spec ended?(session()) :: boolean()
  def ended?(pid), do: not Process.alive?(pid)

  @spec with_transaction(session(), (() -> return)) :: {:ok, return} | {:error, term} when return: term()
  @spec with_transaction(session(), keyword(), (() -> return)) :: {:ok, return} | {:error, term} when return: term()
  def with_transaction(pid, opts \\ [], func) do
    :ok = start_transaction(pid, opts)
    conn = get_connection(pid)
    func.(conn)
  rescue
    exception ->
      abort_transaction(pid)
      reraise exception, __STACKTRACE__
  else
    val ->
      with :ok <- commit_transaction(pid), do: {:ok, val}
  end

  @doc false
  def add_session(query, nil), do: query
  def add_session(query, pid), do: :gen_statem.call(pid, {:add_session, query})

  defp get_connection(pid), do: :gen_statem.call(pid, :get_connection)

  @states [
    :no_transaction,
    :transaction_started,
    :in_transaction,
    :transaction_commited,
    :transaction_aborted
  ]

  @in_txn [:transaction_started, :in_transaction]
  @outside_txn @states -- @in_txn

  def child_spec({topology_pid, id, opts, parent}) do
    %{
      id: nil,
      start: {:gen_statem, :start_link, [__MODULE__, {topology_pid, id, opts, parent}, []]},
      restart: :temporary,
      type: :worker
    }
  end

  def callback_mode, do: :handle_event_function

  def init({topology_pid, id, _opts, parent}) do
    ref = Process.monitor(parent)
    {:ok, :no_transaction, %__MODULE__{id: id, pid: topology_pid, ref: ref}}
  end

  def handle_event({:call, from}, :get_connection, _state, data) do
    {:keep_state_and_data, {:reply, from, data.pid}}
  end

  def handle_event({:call, from}, {:start_transaction, _opts}, state, %{txn: txn} = data)
      when state in @outside_txn do
    {:next_state, :transaction_started, struct(data, txn: txn + 1), {:reply, from, :ok}}
  end

  def handle_event({:call, from}, {:add_session, query}, state, data) when state in @in_txn do
    new_query =
      query
      |> Keyword.new()
      |> Keyword.merge(
        lsid: data.id,
        txnNumber: {:long, data.txn},
        startTransaction: state == :transaction_started,
        autocommit: false
      )

    {:next_state, :in_transaction, data, {:reply, from, new_query}}
  end

  def handle_event({:call, from}, :commit_transaction, state, data) when state in @in_txn do
    response =
      if state == :in_transaction do
        run_txn_command(data, :commitTransaction)
      else
        :ok
      end

    {:next_state, :transaction_commited, data, {:reply, from, response}}
  end

  def handle_event({:call, from}, :abort_transaction, state, data) when state in @in_txn do
    response =
      if state == :in_transaction do
        abort_txn(data)
      else
        :ok
      end

    {:next_state, :transaction_aborted, data, {:reply, from, response}}
  end

  def handle_event({:call, from}, :end_session, _state, _data) do
    {:stop_and_reply, :normal, {:reply, from, :ok}}
  end

  def handle_event(:info, {:DOWN, ref, :process, _pid, _reason}, _state, %{ref: ref}) do
    {:stop, :normal}
  end

  def handle_event({:call, from}, command, state, _data) do
    {:keep_state_and_data, {:reply, from, {:error, {:invalid_call, command, state}}}}
  end

  def terminate(_reason, state, %{pid: pid} = data) do
    if state == :in_transaction, do: :ok = abort_txn(data)

    query = %{
      endSessions: [data.id]
    }

    with {:ok, conn, _, _} <- Mongo.select_server(pid, :write, []),
         do: Mongo.direct_command(conn, query, database: "admin")
  end

  defp abort_txn(data), do: run_txn_command(data, :abortTransaction)

  defp run_txn_command(state, command) do
    query = [
      {command, 1},
      lsid: state.id,
      txnNumber: {:long, state.txn},
      autocommit: false
    ]

    opts = [database: "admin"]

    with {:ok, conn, _, _} <- Mongo.select_server(state.pid, :write, opts),
         {:ok, _} <- Mongo.direct_command(conn, query, opts),
         do: :ok
  end
end
