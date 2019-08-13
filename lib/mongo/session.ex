defmodule Mongo.Session do
  @enforce_keys [:id, :pid]
  defstruct @enforce_keys ++
              [
                :ref,
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

  @doc """
  Start new transaction within current session.
  """
  @spec start_transaction(session()) :: :ok | {:error, term()}
  @spec start_transaction(session(), keyword()) :: :ok | {:error, term()}
  def start_transaction(pid, opts \\ []) do
    :gen_statem.call(pid, {:start_transaction, opts})
  end

  @doc """
  Commit current transaction. It will error if the session is in invalid state.
  """
  @spec commit_transaction(session()) :: :ok | {:error, term}
  def commit_transaction(pid), do: :gen_statem.call(pid, :commit_transaction)

  @doc """
  Abort current transaction and rollback changes introduced by it. It will error
  if the session is invalid.
  """
  @spec abort_transaction(session()) :: :ok | {:error, term()}
  def abort_transaction(pid), do: :gen_statem.call(pid, :abort_transaction)

  @doc """
  Finish current session and rollback uncommited transactions if any.

  **WARNING:** Session is ended in asynchronous manner, which mean, that
  the process itself can be still available and `#{inspect(__MODULE__)}.ended?(session)`
  can still return `false` for some time after calling this function.
  """
  @spec end_session(session()) :: :ok | {:error, term()}
  def end_session(pid) do
    unless ended?(pid), do: :gen_statem.call(pid, :end_session)

    :ok
  end

  @doc """
  Check whether given session has already ended.
  """
  @spec ended?(session()) :: boolean()
  def ended?(pid), do: not Process.alive?(pid)

  @doc """
  Run provided `func` within transaction and automatically commit it if there
  was no exceptions.
  """
  @spec with_transaction(session(), (GenServer.server() -> return)) ::
          {:ok, return} | {:error, term}
        when return: term()
  @spec with_transaction(session(), keyword(), (GenServer.server() -> return)) ::
          {:ok, return} | {:error, term}
        when return: term()
  def with_transaction(pid, opts \\ [], func) do
    :ok = start_transaction(pid, opts)
    conn = get_connection(pid)
    func.(conn)
  rescue
    exception ->
      abort_transaction(pid)
      reraise exception, System.stacktrace()
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

  @doc false
  def child_spec({topology_pid, id, opts, parent}) do
    casual_consistency = Keyword.get(opts, :casual_consistency, true)
    read_concern = Keyword.get(opts, :read_concern, %{})
    read_preference = Keyword.get(opts, :read_preference)
    retry_writes = Keyword.get(opts, :retry_writes, true)
    write_concern = Keyword.get(opts, :write_concern)

    state = %__MODULE__{
      id: id,
      pid: topology_pid,
      casual_consistency: casual_consistency,
      read_concern: read_concern,
      read_preference: read_preference,
      retry_writes: retry_writes,
      write_concern: write_concern
    }

    %{
      id: nil,
      start: {:gen_statem, :start_link, [__MODULE__, {parent, state}, []]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  @impl :gen_statem
  def init({parent, state}) do
    ref = Process.monitor(parent)
    {:ok, :no_transaction, struct(state, ref: ref)}
  end

  @impl :gen_statem
  # Get current connection form session.
  def handle_event({:call, from}, :get_connection, _state, data) do
    {:keep_state_and_data, {:reply, from, data.pid}}
  end

  # Start new transaction if there isn't one already.
  def handle_event({:call, from}, {:start_transaction, _opts}, state, %{txn: txn} = data)
      when state in @outside_txn do
    {:next_state, :transaction_started, struct(data, txn: txn + 1), {:reply, from, :ok}}
  end

  # Add session information to the query metadata.
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
      |> add_option(:writeConcern, data.write_concern)
      |> add_option(:readConcern, data.read_concern)

    {:next_state, :in_transaction, data, {:reply, from, new_query}}
  end

  # Commit transaction. If there isn't any then just change current state to
  # `transaction_commited` and call it a day.
  def handle_event({:call, from}, :commit_transaction, state, data) when state in @in_txn do
    response =
      if state == :in_transaction do
        run_txn_command(data, :commitTransaction)
      else
        :ok
      end

    {:next_state, :transaction_commited, data, {:reply, from, response}}
  end

  # Abort transaction if there is any. If there is none then change state to
  # `transaction_aborted`
  def handle_event({:call, from}, :abort_transaction, state, data) when state in @in_txn do
    response =
      if state == :in_transaction do
        abort_txn(data)
      else
        :ok
      end

    {:next_state, :transaction_aborted, data, {:reply, from, response}}
  end

  # Finish session by ending process (for further "closing" see `terminate/3`
  # handler.
  def handle_event({:call, from}, :end_session, _state, _data) do
    {:stop_and_reply, :normal, {:reply, from, :ok}}
  end

  # If parent process died before session then stop process and handle aborting
  # sessions in `terminate/3` handler.
  def handle_event(:info, {:DOWN, ref, :process, _pid, _reason}, _state, %{ref: ref}) do
    {:stop, :normal}
  end

  # On unsupported call (for example call in invalid state) just return error to
  # the caller with information about current state and called command.
  def handle_event({:call, from}, command, state, _data) do
    {:keep_state_and_data, {:reply, from, {:error, {:invalid_call, command, state}}}}
  end

  @impl :gen_statem
  # Abort all pending transactions if there any and end session itself.
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
    query =
      [
        {command, 1},
        lsid: state.id,
        txnNumber: {:long, state.txn},
        autocommit: false
      ]
      |> add_option(:writeConcern, state.write_concern)

    opts = [database: "admin"]

    with {:ok, conn, _, _} <- Mongo.select_server(state.pid, :write, opts),
         {:ok, _} <- Mongo.direct_command(conn, query, opts),
         do: :ok
  end

  defp add_option(conn_opts, _key, nil), do: conn_opts

  defp add_option(conn_opts, key, value) do
    List.keydelete(conn_opts, key, 0) ++ [{key, value}]
  end
end
