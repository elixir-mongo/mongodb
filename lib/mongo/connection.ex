defmodule Mongo.Connection do
  use GenServer
  import Mongo.Protocol

  @timeout 5000

  def start_link(opts) do
    opts = Keyword.put_new(opts, :timeout,  @timeout)
    GenServer.start_link(__MODULE__, opts)
  end

  @doc false
  def init(opts) do
    GenServer.cast(self, :connect)
    {:ok, %{socket: nil, auth: [], tail: "", queue: %{}, request_id: 0,
            opts: opts, database: opts[:database]}}
  end

  @doc false
  def handle_call({:auth, database, username, password}, from, s) do
    s = auth(database, username, password, from, s)
    {:noreply, s}
  end

  def handle_call(:database, _from, %{database: database} = s) do
    {:reply, database, s}
  end

  def handle_call({:database, database}, _from, s) do
    {:reply, :ok, %{s | database: database}}
  end

  @doc false
  def handle_cast(:connect, %{opts: opts} = s) do
    host      = Keyword.fetch!(opts, :hostname)
    host      = if is_binary(host), do: String.to_char_list(host), else: host
    port      = opts[:port] || 27017
    timeout   = opts[:timeout] || @timeout
    sock_opts = [:binary, active: :once, packet: :raw]
                ++ (opts[:socket_options] || [])

    case :gen_tcp.connect(host, port, sock_opts, timeout) do
      {:ok, socket} ->
        s = auth(%{s | socket: socket})
        {:noreply, s}

      {:error, reason} ->
        {:stop, %Mongo.Error{message: "tcp connect: #{reason}"}, s}
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
    {:stop, %Mongo.Error{message: "tcp closed"}, s}
  end

  def handle_info({:tcp_error, _, reason}, s) do
    {:stop, %Mongo.Error{message: "tcp error: #{reason}"}, s}
  end

  defp new_data(data, %{tail: tail} = s) do
    data = tail <> data

    case decode(data) do
      {:ok, id, reply, tail} ->
        state = s.queue[id][:state]
        s = %{s | tail: tail}

        if :query_failure in op_reply(reply, :flags),
            do: failure(state, id, reply, s),
          else: reply(state, id, reply, s)

      :error ->
        {:ok, %{s | tail: data}}
    end
  end

  defp reply(:nonce, id, op_reply(docs: [%{"nonce" => nonce, "ok" => 1.0}]), s) do
    {database, username, password} = s.queue[id].params
    digest = digest(nonce, username, password)
    doc = %{authenticate: 1, user: username, nonce: nonce, key: digest}

    find_one(id, [database, ?., "$cmd"], doc, nil, s)
    {:ok, state(id, :auth, s)}
  end

  defp reply(:auth, id, op_reply(docs: [%{"ok" => 1.0}]), s) do
    unless s.database do
      {database, _, _} = s.queue[id].params
      s = %{s | database: database}
    end

    s = reply(:ok, id, s)
    {:ok, s}
  end

  defp failure(_state, id, op_reply(docs: [%{"$err" => reason}]), s) do
    error = %Mongo.Error{message: reason}
    if reply(error, id, s),
        do: {:ok, s},
      else: {:error, error, s}
  end

  defp auth(%{opts: opts} = s) do
    database = opts[:database]
    username = opts[:username]
    password = opts[:password]
    auth     = opts[:auth]

    s = %{s | opts: Keyword.drop(opts, [:database, :username, :password, :auth])}

    cond do
      database && username && password ->
        auth(database, username, password, nil, s)
      auth ->
        Enum.reduce(auth, s, fn opts, s ->
          auth(opts[:database], opts[:username], opts[:password], nil, s)
        end)
    end
  end

  defp auth(database, username, password, from, s) do
    {id, s} = new_command(:nonce, {database, username, password}, from, s)
    find_one(id, "$cmd", %{getnonce: 1}, nil, s)
    s
  end

  defp find_one(request_id, coll, query, select, s) do
    op_query(coll: namespace(coll, s), query: query, select: select,
             num_skip: 0, num_return: 1, flags: [])
    |> send(request_id, s)
  end

  defp send(op, request_id, s) do
    data = encode(request_id, op)
    :gen_tcp.send(s.socket, data)
  end

  defp digest(nonce, username, password) do
    :crypto.hash(:md5, [nonce, username, digest_password(username, password)])
    |> Base.encode16(case: :lower)
  end

  defp digest_password(username, password) do
    :crypto.hash(:md5, [username, ":mongo:", password])
    |> Base.encode16(case: :lower)
  end

  defp namespace(coll, s) when is_binary(coll) do
    if :binary.match(coll, ".") == :nomatch do
      [s.database, ?. | coll]
    else
      coll
    end
  end

  defp namespace(coll, _s) when is_list(coll) do
    coll
  end

  defp new_command(state, params, from, s) do
    command = %{state: state, params: params, from: from}
    queue   = Map.put(s.queue, s.request_id, command)
    {s.request_id, %{s | request_id: s.request_id+1, queue: queue}}
  end

  defp state(id, state, s) do
    put_in(s.queue[id].state, state)
  end

  defp reply(reply, id, s) do
    case Map.fetch(s, id) do
      {:ok, %{from: nil}} ->
        false
      {:ok, %{from: from}} ->
        reply(reply, from)
        true
      :error ->
        false
    end

    %{s | queue: Map.delete(s.queue, id)}
  end

  defp reply(reply, {_, _} = from) do
    GenServer.reply(from, reply)
  end
end
