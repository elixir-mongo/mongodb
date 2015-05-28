defmodule Mongo.Connection do
  import Mongo.Protocol
  require Logger

  @behaviour Connection
  @backoff 1000
  @timeout 5000
  @requestid_max 2147483648

  def start_link(opts) do
    opts = Keyword.put_new(opts, :timeout,  @timeout)
    Connection.start_link(__MODULE__, opts)
  end

  @doc false
  def init(opts) do
    s = %{socket: nil, auth: nil, tail: "", queue: %{}, request_id: 0,
          opts: opts, database: nil, timeout: opts[:timeout] || @timeout}
    s = setup_auth(s)
    {:connect, :init, s}
  end

  @doc false
  def connect(:init, %{opts: opts} = s) do
    host      = Keyword.fetch!(opts, :hostname)
    host      = if is_binary(host), do: String.to_char_list(host), else: host
    port      = opts[:port] || 27017
    sock_opts = [:binary, active: false, packet: :raw]
                ++ (opts[:socket_options] || [])

    case :gen_tcp.connect(host, port, sock_opts, s.timeout) do
      {:ok, socket} ->
        s = %{s | socket: socket}

        case init_auth(s) do
          :ok ->
            :inet.setopts(socket, active: :once)
            {:ok, s}
          {:error, reason} ->
            {:stop, reason, s}
          {:tcp_error, reason} ->
            Logger.error "Mongo tcp error (#{host}:#{port}): #{inspect reason}"
            {:backoff, @backoff, s}
        end

      {:error, reason} ->
        Logger.error "Mongo connect error (#{host}:#{port}): #{inspect reason}"
        {:backoff, @backoff, s}
    end
  end

  @doc false
  def disconnect({:error, reason}, s) do
    host = s.opts[:hostname]
    port = s.opts[:port] || 27017
    Logger.error "Mongo tcp error (#{host}:#{port}): #{inspect reason}"
    {:backoff, 0, %{s | socket: nil}}
  end

  @doc false
  def handle_cast(msg, _) do
    exit({:bad_cast, msg})
  end

  @doc false
  def handle_call(_, _from, %{socket: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:auth, database, username, password}, from, s) do
    {id, s} = new_command(:nonce, {database, username, password}, from, s)
    find_one(id, "$cmd", %{getnonce: 1}, nil, s)
    |> send_to_noreply
  end

  def handle_call(:database, _from, %{database: database} = s) do
    {:reply, database, s}
  end

  def handle_call({:database, database}, _from, s) do
    {:reply, :ok, %{s | database: database}}
  end

  def handle_call({:find_one, coll, query, select}, from, s) do
    {id, s} = new_command(:one, nil, from, s)
    find_one(id, coll, query, select, s)
    |> send_to_noreply
  end

  def handle_call({:insert, coll, docs}, _from, s) do
    op_insert(coll: namespace(coll, s), docs: List.wrap(docs), flags: [])
    |> send(-10, s)
    |> send_to_reply(:ok)
  end

  @doc false
  def handle_info({:tcp, _, data}, %{socket: socket, tail: tail} = s) do
    case new_data(tail <> data, s) do
      {:ok, s} ->
        :inet.setopts(socket, active: :once)
        {:noreply, s}
      {:error, error, s} ->
        {:stop, error, s}
    end
  end

  def handle_info({:tcp_closed, _}, s) do
    # TODO: Disconnect so we can reconnect (check example)
    {:stop, %Mongo.Error{message: "tcp closed"}, s}
  end

  def handle_info({:tcp_error, _, reason}, s) do
    # TODO: Disconnect so we can reconnect (check example)
    {:stop, %Mongo.Error{message: "tcp error: #{reason}"}, s}
  end

  def code_change(_, s, _) do
    {:ok, s}
  end

  def terminate(_, _) do
    :ok
  end

  defp new_data(data, %{tail: tail} = s) do
    data = tail <> data

    case decode(data) do
      {:ok, id, reply, tail} ->
        state = s.queue[id][:state]
        s = %{s | tail: tail}

        if :query_failure in op_reply(reply, :flags),
            do: failure(state, id, reply, s),
          else: message(state, id, reply, s)

      :error ->
        {:ok, %{s | tail: data}}
    end
  end

  defp message(:nonce, id, op_reply(docs: [%{"nonce" => nonce, "ok" => 1.0}]), s) do
    {database, username, password} = s.queue[id].params
    digest = digest(nonce, username, password)
    doc = %{authenticate: 1, user: username, nonce: nonce, key: digest}
    s = state(id, :auth, s)

    find_one(id, {database, "$cmd"}, doc, nil, s)
    |> send_to_noreply
  end

  defp message(:auth, id, op_reply(docs: [%{"ok" => 1.0}]), s) do
    unless s.database do
      {database, _, _} = s.queue[id].params
      s = %{s | database: database}
    end

    s = reply(:ok, id, s)
    {:ok, s}
  end

  defp message(:auth, id, op_reply(docs: [%{"ok" => 0.0, "errmsg" => reason, "code" => code}]), s) do
    reply_error(id, %Mongo.Error{message: "authentication failed: #{reason}", code: code}, s)
  end

  defp message(:auth, id, op_reply(docs: []), s) do
    reply_error(id, %Mongo.Error{message: "authentication failed"}, s)
  end

  defp message(:one, id, op_reply(docs: docs), s) do
    case docs do
      [doc] -> {:ok, reply(doc, id, s)}
      []    -> {:ok, reply(nil, id, s)}
    end
  end

  defp failure(_state, id, op_reply(docs: [%{"$err" => reason, "code" => code }]), s) do
    reply_error(id, %Mongo.Error{message: reason, code: code}, s)
  end

  defp find_one(id, coll, query, select, s) do
    op_query(coll: namespace(coll, s), query: query, select: select,
             num_skip: 0, num_return: 1, flags: [])
    |> send(id, s)
  end

  defp send(op, id, s) do
    data = encode(id, op)
    case :gen_tcp.send(s.socket, data) do
      :ok ->
        {:ok, s}
      {:error, _} = error ->
        error
    end
  end

  defp digest(nonce, username, password) do
    :crypto.hash(:md5, [nonce, username, digest_password(username, password)])
    |> Base.encode16(case: :lower)
  end

  defp digest_password(username, password) do
    :crypto.hash(:md5, [username, ":mongo:", password])
    |> Base.encode16(case: :lower)
  end

  defp namespace({database, coll}, _s) do
    [database, ?. | coll]
  end

  defp namespace(coll, s) do
    [s.database, ?. | coll]
  end

  defp new_command(state, params, from, s) do
    command    = %{state: state, params: params, from: from}
    queue      = Map.put(s.queue, s.request_id, command)
    request_id = rem s.request_id+1, @requestid_max

    {s.request_id, %{s | request_id: request_id, queue: queue}}
  end

  defp state(id, state, s) do
    put_in(s.queue[id].state, state)
  end

  defp reply_error(id, error, s) do
    command = Map.fetch(s.queue, id)
    s = %{s | queue: Map.delete(s.queue, id)}

    case command do
      {:ok, %{from: nil}} ->
        {:error, error, s}
      {:ok, %{from: from}} ->
        reply(error, from)
        {:ok, s}
      :error ->
        {:error, error, s}
    end
  end

  defp reply(reply, id, s) do
    case Map.fetch(s.queue, id) do
      {:ok, %{from: nil}}  -> :ok
      {:ok, %{from: from}} -> reply(reply, from)
      :error               -> :ok
    end

    %{s | queue: Map.delete(s.queue, id)}
  end

  defp reply(reply, {_, _} = from) do
    GenServer.reply(from, reply)
  end

  defp send_to_noreply({:ok, s}),
    do: {:noreply, s}
  defp send_to_noreply({:error, reason, s}),
    do: {:disconnect, {:error, reason}, s}

  defp send_to_reply({:ok, s}, reply),
    do: {:reply, reply, s}
  defp send_to_reply({:error, reason, s}, reply),
    do: {:disconnect, {:error, reason}, reply, s}

  defp setup_auth(%{auth: nil, opts: opts} = s) do
    database = opts[:database]
    username = opts[:username]
    password = opts[:password]
    auth     = opts[:auth] || []

    auth =
      Enum.map(auth, fn opts ->
        database = opts[:database]
        username = opts[:username]
        password = opts[:password]
        {database, username, password}
      end)

    if database && username && password do
      auth = auth ++ [{database, username, password}]
    end

    if auth != [] do
      database = s.database || (auth |> List.last |> elem(0))
    end

    opts = Keyword.drop(opts, ~w(database username password auth)a)
    %{s | auth: auth, opts: opts, database: database}
  end

  defp init_auth(%{auth: auth} = s) do
    Enum.find_value(auth, fn opts ->
      case inactive_auth(opts, s) do
        :ok ->
          nil
        {:error, _} = error ->
          error
      end
    end) || :ok
  end

  defp inactive_auth({database, username, password}, s) do
    case inactive_command(-1, database, %{getnonce: 1}, s) do
      {:ok, %{"nonce" => nonce, "ok" => 1.0}} ->
        inactive_digest(nonce, database, username, password, s)
      {:tcp_error, _} = error ->
        error
    end
  end

  defp inactive_digest(nonce, database, username, password, s) do
    digest = digest(nonce, username, password)
    command = %{authenticate: 1, user: username, nonce: nonce, key: digest}

    case inactive_command(-2, database, command, s) do
      {:ok, %{"ok" => 1.0}} ->
        :ok
      {:ok, %{"ok" => 0.0, "errmsg" => reason, "code" => code}} ->
        {:error, %Mongo.Error{message: "auth failed for '#{username}': #{reason}", code: code}}
      {:ok, nil} ->
        {:error, %Mongo.Error{message: "auth failed for '#{username}'"}}
      {:tcp_error, _} = error ->
        error
    end
  end

  defp inactive_command(id, database, command, s) do
    case find_one(id, {database, "$cmd"}, command, nil, s) do
      {:ok, s} ->
        case inactive_recv(s) do
          {:ok, ^id, reply} ->
            case reply do
              op_reply(docs: [doc]) -> {:ok, doc}
              op_reply(docs: [])    -> {:ok, nil}
            end
          {:tcp_error, _} = error ->
            error
        end
      {:error, reason} ->
        {:tcp_error, reason}
    end
  end

  defp inactive_recv(tail \\ "", s) do
    case :gen_tcp.recv(s.socket, 0, s.timeout) do
      {:ok, data} ->
        data = tail <> data
        case decode(data) do
          {:ok, id, reply, ""} ->
            {:ok, id, reply}
          :error ->
            inactive_recv(data, s)
        end

      {:error, reason} ->
        {:tcp_error, reason}
    end
  end
end
