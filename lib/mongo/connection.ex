defmodule Mongo.Connection do
  import Mongo.Protocol
  import Kernel, except: [send: 2]
  require Logger

  @behaviour Connection
  @backoff 1000
  @timeout 5000
  @requestid_max 2147483648
  @write_concern ~w(w j fsync wtimeout)a

  def start_link(opts) do
    opts = Keyword.put_new(opts, :timeout,  @timeout)
    Connection.start_link(__MODULE__, opts)
  end

  @doc false
  def init(opts) do
    timeout = opts[:timeout] || @timeout

    opts = opts
           |> Keyword.update!(:hostname, &to_char_list/1)
           |> Keyword.put_new(:port, 27017)
           |> Keyword.delete(:timeout)

    {write_concern, opts} = Keyword.split(opts, @write_concern)

    write_concern = Enum.into(write_concern, %{})
                    |> Map.put_new(:w, 1)

    s = %{socket: nil, auth: nil, tail: "", queue: %{}, request_id: 0,
          opts: opts, database: nil, timeout: timeout,
          write_concern: write_concern}

    s = setup_auth(s)
    {:connect, :init, s}
  end

  @doc false
  def connect(_, %{opts: opts} = s) do
    host = opts[:hostname]
    port = opts[:port]
    sock_opts = [:binary, active: false, packet: :raw, send_timeout: s.timeout]
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
            Logger.error "Mongo tcp error (#{host}:#{port}): #{format_error(reason)}"
            {:backoff, @backoff, s}
        end

      {:error, reason} ->
        Logger.error "Mongo connect error (#{host}:#{port}): #{format_error(reason)}"
        {:backoff, @backoff, s}
    end
  end

  @doc false
  def disconnect({:error, reason}, s) do
    # TODO: Reply to everyone in queue and reset it

    host = s.opts[:hostname]
    port = s.opts[:port] || 27017
    Logger.error "Mongo tcp error (#{host}:#{port}): #{format_error(reason)}"

    # Backoff 0 to churn through all commands in mailbox before reconnecting
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
    find_one("$cmd", %{getnonce: 1}, nil, s)
    |> send(id, s)
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
    find_one(coll, query, select, s)
    |> send(id, s)
    |> send_to_noreply
  end

  def handle_call({:insert, coll, docs, opts}, from, s) do
    insert_op = {-10, op_insert(coll: namespace(coll, s), docs: List.wrap(docs),
                                flags: flags(opts))}

    if s.write_concern.w == 0 do
      insert_op |> send(s) |> send_to_reply(:ok)
    else
      {id, s} = new_command(:insert, nil, from, s)
      command = Map.merge(%{getLastError: 1}, s.write_concern)

      [insert_op,
       {id, find_one({:override, coll, "$cmd"}, command, nil, s)}]
      |> send(s)
      |> send_to_noreply
    end
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
    {:disconnect, {:error, :closed}, s}
  end

  def handle_info({:tcp_error, _, reason}, s) do
    {:disconnect, {:error, reason}, s}
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

    find_one({database, "$cmd"}, doc, nil, s)
    |> send(id, s)
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

  defp message(:insert, id, op_reply(docs: docs), s) do
    case docs do
      [%{"ok" => 1.0}] ->
        s = reply(:ok, id, s)
      [%{"ok" => 0.0, "err" => reason, "code" => code}] ->
        s = reply({:error, %Mongo.Error{message: reason, code: code}}, id, s)
    end
    {:ok, s}
  end

  defp failure(_state, id, op_reply(docs: [%{"$err" => reason, "code" => code }]), s) do
    reply_error(id, %Mongo.Error{message: reason, code: code}, s)
  end

  defp find_one(coll, query, select, s) do
    op_query(coll: namespace(coll, s), query: query, select: select,
             num_skip: 0, num_return: 1, flags: [])
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

  defp send(ops, s) do
    data =
      Enum.reduce(ops, "", fn {id, op}, acc ->
        [acc|encode(id, op)]
      end)

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


  defp namespace({:override, {database, _}, coll}, _s),
    do: [database, ?. | coll]
  defp namespace({:override, _, coll}, s),
    do: [s.database, ?. | coll]
  defp namespace({database, coll}, _s),
    do: [database, ?. | coll]
  defp namespace(coll, s),
    do: [s.database, ?. | coll]

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

  defp format_error(:closed),
    do: "closed"
  defp format_error(error),
    do: :inet.format_error(error)

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
    case find_one({database, "$cmd"}, command, nil, s) |> send(id, s) do
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

  defp flags(flags) do
    Enum.reduce(flags, [], fn
      {flag, true},   acc -> [flag|acc]
      {_flag, false}, acc -> acc
    end)
  end
end
