defmodule Mongo.Session do
  @enforce_keys [:id, :pid]
  defstruct @enforce_keys ++
              [
                :ref,
                :read_concern,
                :write_concern,
                :read_preference,
                operation_time: nil,
                causal_consistency: true,
                retry_writes: false,
                txn: %{
                  seq: 0,
                  read_concern: nil,
                  write_concern: nil
                }
              ]

  @opaque session :: pid()

  # 10 minute timeout
  @timeout {:state_timeout, 10 * 60 * 60, nil}

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
  @spec end_session(session()) :: :ok
  def end_session(pid) do
    Mongo.SessionPool.checkin(pid)

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
  @spec with_transaction(session(), (() -> return)) ::
          {:ok, return} | {:error, term}
        when return: term()
  @spec with_transaction(session(), keyword(), (() -> return)) ::
          {:ok, return} | {:error, term}
        when return: term()
  def with_transaction(pid, opts \\ [], func) do
    :ok = start_transaction(pid, opts)

    func.()
  rescue
    exception ->
      _ = abort_transaction(pid)
      reraise exception, System.stacktrace()
  else
    val ->
      with :ok <- commit_transaction(pid), do: {:ok, val}
  end

  @doc false
  def advance_operation_time(pid, timestamp) do
    :gen_statem.call(pid, {:advance_operation_time, timestamp})
  end

  @doc false
  def set_options(pid, opts) do
    causal_consistency = Keyword.get(opts, :causal_consistency, true)
    read_concern = Keyword.get(opts, :read_concern, %{})
    read_preference = Keyword.get(opts, :read_preference)
    retry_writes = Keyword.get(opts, :retry_writes, true)
    write_concern = Keyword.get(opts, :write_concern)

    state = %{
      causal_consistency: causal_consistency,
      read_concern: read_concern,
      read_preference: read_preference,
      retry_writes: retry_writes,
      write_concern: write_concern
    }

    :gen_statem.call(pid, {:set_options, state})
  end

  @doc false
  def update_session(doc, nil), do: doc

  def update_session(%{"operationTime" => operation_ts} = doc, pid) do
    :ok = advance_operation_time(pid, operation_ts)

    doc
  end

  def update_session(doc, _pid), do: doc

  @doc false
  def add_session(query, nil), do: {:ok, query}
  def add_session(query, pid), do: :gen_statem.call(pid, {:add_session, query})

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
    causal_consistency = Keyword.get(opts, :causal_consistency, true)
    read_concern = Keyword.get(opts, :read_concern, %{})
    read_preference = Keyword.get(opts, :read_preference)
    retry_writes = Keyword.get(opts, :retry_writes, true)
    write_concern = Keyword.get(opts, :write_concern)

    state = %__MODULE__{
      id: id,
      pid: topology_pid,
      causal_consistency: causal_consistency,
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

  if String.to_integer(System.otp_release()) < 20 do
    @impl :gen_statem
    def init({parent, state}) do
      ref = Process.monitor(parent)
      {:handle_event_function, :no_transaction, struct(state, ref: ref)}
    end
  else
    @impl :gen_statem
    def callback_mode, do: :handle_event_function

    @impl :gen_statem
    def init({parent, state}) do
      ref = Process.monitor(parent)
      {:ok, :no_transaction, struct(state, ref: ref)}
    end
  end

  @impl :gen_statem
  # Get current connection form session.
  def handle_event({:call, from}, :get_connection, _state, data) do
    {:keep_state_and_data, {:reply, from, data.pid}}
  end

  # Start new transaction if there isn't one already.
  def handle_event({:call, from}, {:start_transaction, opts}, state, %{txn: %{seq: seq}} = data)
      when state in @outside_txn do
    write_concern = Keyword.get(opts, :write_concern, data.write_concern)
    read_concern = Keyword.get(opts, :read_concern, data.read_concern)

    txn = %{
      seq: seq + 1,
      write_concern: write_concern,
      read_concern: read_concern
    }

    {:next_state, :transaction_started, struct(data, txn: txn), {:reply, from, :ok}}
  end

  # Add session information to the query metadata.
  def handle_event({:call, from}, {:add_session, query}, :transaction_started, data) do
    %{
      txn: %{
        seq: seq,
        read_concern: read_concern,
        write_concern: write_concern
      }
    } = data

    new_query =
      query
      |> Keyword.new()
      |> add_option(:lsid, data.id)
      |> add_option(:txnNumber, {:long, seq})
      |> add_option(:startTransaction, true)
      |> add_option(:autocommit, false)
      |> add_option(:writeConcern, write_concern)
      |> add_option(:readConcern, read_concern)
      |> set_read_concern(data.operation_time, data.causal_consistency)

    {:next_state, :in_transaction, data, {:reply, from, {:ok, new_query}}}
  end

  def handle_event({:call, from}, {:add_session, query}, :in_transaction, data) do
    new_query =
      query
      |> Keyword.new()
      |> Keyword.merge(
        lsid: data.id,
        txnNumber: {:long, data.txn},
        autocommit: false
      )

    case Keyword.get(new_query, :read_preference, %{mode: :primary}) do
      %{mode: :primary} ->
        {:keep_state_and_data, {:reply, from, {:ok, new_query}}}

      %{mode: mode} ->
        {:keep_state_and_data,
         {:reply, from,
          {:error,
           Mongo.Error.exception(message: "Read preference must be primary, not: #{mode}")}}}
    end
  end

  def handle_event({:call, from}, {:add_session, query}, _state, data) do
    if query[:will_retry_write] do
      handle_event({:call, from}, {:add_session, query}, :in_transaction, data)
    else
      new_query =
        query
        |> Keyword.new()
        |> add_option(:lsid, data.id)
        |> set_read_concern(data.operation_time, data.causal_consistency)

      {:next_state, :no_transaction, data, {:reply, from, {:ok, new_query}}}
    end
  end

  # Commit transaction. If there isn't any then just change current state to
  # `transaction_commited` and call it a day.
  def handle_event({:call, from}, :commit_transaction, state, data)
      when state in @in_txn or state == :transaction_commited do
    return =
      if state == :in_transaction do
        try_run_txn_command(data, :commitTransaction)
      else
        :ok
      end

    {:next_state, :transaction_commited, data, [{:reply, from, return}, @timeout]}
  end

  # Abort transaction if there is any. If there is none then change state to
  # `transaction_aborted`
  def handle_event({:call, from}, :abort_transaction, state, data) when state in @in_txn do
    response =
      if state == :in_transaction do
        try_run_txn_command(data, :abortTransaction)
      else
        :ok
      end

    {:next_state, :transaction_aborted, data, [{:reply, from, response}, @timeout]}
  end

  # Finish session by ending process (for further "closing" see `terminate/3`
  # handler.
  def handle_event({:call, from}, :end_session, _state, _data) do
    {:stop_and_reply, :normal, {:reply, from, :ok}}
  end

  def handle_event({:call, from}, {:advance_operation_time, timestamp}, _state, data) do
    if not is_nil(data.operation_time) and
         (timestamp.value > data.operation_time.value or
            (timestamp.value == data.operation_time.value and
               timestamp.ordinal > data.operation_time.ordinal)) do
      {:keep_state, struct(data, operation_time: timestamp), [{:reply, from, :ok}, @timeout]}
    else
      {:keep_state_and_data, [{:reply, from, :ok}, @timeout]}
    end
  end

  def handle_event({:call, from}, {:set_options, opts}, _state, data) do
    {:keep_state, struct(data, opts), {:reply, from, :ok}}
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

  def handle_event(:state_timeout, _, _, _), do: {:stop, :normal}

  @impl :gen_statem
  # Abort all pending transactions if there any and end session itself.
  def terminate(_reason, state, %{pid: pid} = data) do
    _ =
      if state == :in_transaction do
        try_run_txn_command(data, :abortTransaction)
      end

    query = %{
      endSessions: [data.id]
    }

    with {:ok, conn, _, _} <- Mongo.select_server(pid, :write, []),
         do: Mongo.direct_command(conn, query, database: "admin")
  end

  defp try_run_txn_command(data, command) do
    case run_txn_command(data, command) do
      :ok ->
        :ok

      {:error, error} = val ->
        if Mongo.Error.retryable(error) do
          new_data =
            Map.update!(data, :write_concern, fn
              nil ->
                %{w: :majority, wtimeout: 10_000}

              map when is_map(map) ->
                map
                |> Map.put(:w, :majority)
                |> Map.put_new(:wtimeout, 10_000)
            end)

          try_run_txn_command(new_data, command)
        else
          val
        end
    end
  end

  defp run_txn_command(state, command) do
    query =
      [
        {command, 1},
        lsid: state.id,
        autocommit: false,
        txnNumber: {:long, state.txn.seq}
      ]
      |> add_option(:writeConcern, state.write_concern)

    opts = [database: "admin"]

    with {:ok, conn, _, _} <- Mongo.select_server(state.pid, :write, opts),
         {:ok, _} <- Mongo.direct_command(conn, query, opts),
         do: :ok
  end

  defp set_read_concern(conn_opts, _, false), do: conn_opts

  defp set_read_concern(conn_opts, nil, true) do
    add_option(conn_opts, :readConcern, %{})
  end

  defp set_read_concern(conn_opts, time, true) do
    Keyword.update(
      conn_opts,
      :readConcern,
      %{afterClusterTime: time},
      &Map.put(&1, :afterClusterTime, time)
    )
  end

  defp add_option(conn_opts, _key, nil), do: conn_opts

  defp add_option(conn_opts, key, value) do
    List.keydelete(conn_opts, key, 0) ++ [{key, value}]
  end
end
