defmodule Mongo.Session do
  defstruct [:id, :conn, :opts, txn: 0]

  @behaviour :gen_statem

  def start_link(conn, id, opts) do
    :gen_statem.start_link(__MODULE__, {conn, id, opts}, [])
  end

  def start_transaction(pid, opts \\ []) do
    :gen_statem.call(pid, {:start_transaction, opts})
  end

  def commit_transaction(pid, opts \\ []) do
    :gen_statem.call(pid, {:commit_transaction, opts})
  end

  def abort_transaction(pid, opts \\ []) do
    :gen_statem.call(pid, {:abort_transaction, opts})
  end

  def end_session(pid, opts \\ []) do
    :gen_statem.call(pid, {:end_session, opts})
  end

  @doc false
  def add_session(query, nil), do: query
  def add_session(query, pid), do: :gen_statem.call(pid, {:add_session, query})

  def ended?(pid), do: Process.alive?(pid)

  def get_connection(pid) do
    :gen_statem.call(pid, :get_connection)
  end

  def with_transaction(pid, opts \\ [], func) do
    try do
      :ok = start_transaction(pid, opts)
      func.(pid)
    rescue
      exception ->
        abort_transaction(pid, opts)
        reraise exception, __STACKTRACE__
    else
      val ->
        :ok = commit_transaction(pid, opts)

      val
    end
  end

  def callback_mode, do: :handle_event_function

  def init({conn, id, opts}) do
    {:ok, :no_transaction, %__MODULE__{id: id, conn: conn, opts: opts}}
  end

  def handle_event({:call, from}, :get_connection, _state, data) do
    {:keep_state_and_data, {:reply, from, data.conn}}
  end

  def handle_event({:call, from}, {:start_transaction, _opts}, state, %{txn: txn} = data)
      when state in [:no_transaction, :transaction_commited] do
    {:next_state, :transaction_started, struct(data, txn: txn + 1), {:reply, from, :ok}}
  end

  def handle_event({:call, from}, {:commit_transaction, _}, :transaction_started, _data) do
    {:keep_state_and_data, {:reply, from, :ok}}
  end

  def handle_event({:call, from}, {:commit_transaction, opts}, :in_transaction, data) do
    response = run_txn_command(data, :commitTransaction, opts)

    {:next_state, :transaction_commited, data, {:reply, from, response}}
  end

  def handle_event({:call, from}, {:add_session, query}, :transaction_started, data) do
    new_query =
      query
      |> Keyword.new()
      |> Keyword.merge(
        lsid: data.id,
        txnNumber: 2_147_483_647 + data.txn,
        startTransaction: true,
        autocommit: false
      )

    {:next_state, :in_transaction, data, {:reply, from, new_query}}
  end

  def handle_event({:call, from}, {:add_session, query}, :in_transaction, data) do
    new_query =
      query
      |> Keyword.new()
      |> Keyword.merge(
        lsid: data.id,
        txnNumber: 2_147_483_647 + data.txn,
        startTransaction: false,
        autocommit: false
      )

    {:keep_state_and_data, {:reply, from, new_query}}
  end

  def handle_event({:call, from}, {:abort_transaction, opts}, :in_transaction, data) do
    response = abort_txn(data, opts)

    {:next_state, :transaction_commited, data, {:reply, from, response}}
  end

  def handle_event({:call, from}, {:end_session, _}, _state, _data) do
    {:stop_and_reply, :normal, {:reply, from, :ok}}
  end

  def terminate(_reason, state, %{conn: conn} = data) do
    if state == :in_transaction do
      :ok = abort_txn(data, [])
    end

    {:ok, _} = Mongo.direct_command(conn, %{endSessions: [data.id]})
  end

  defp abort_txn(data, opts) do
    run_txn_command(data, :abortTransaction, opts)
  end

  defp run_txn_command(%{conn: conn, txn: txn, id: id}, command, opts) do
    command = [
      {command, 1},
      lsid: id,
      writeConcern: %{w: 1},
      txnNumber: 2_147_483_647 + txn,
      autocommit: false
    ]

    case Mongo.direct_command(conn, command, opts) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end
end
