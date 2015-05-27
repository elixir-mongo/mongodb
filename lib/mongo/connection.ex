defmodule Mongo.Connection do
  use GenServer
  import Mongo.Protocol
  import Mongo.Utils

  @timeout 5000
  @nonce_rid 0
  @auth_rid 1
  @first_rid 2

  def start_link(opts) do
    opts = Keyword.put_new(opts, :timeout,  @timeout)
    GenServer.start_link(__MODULE__, opts)
  end

  @doc false
  def init(opts) do
    GenServer.cast(self, :connect)
    {:ok, %{socket: nil, opts: opts, tail: "", queue: %{}, request_id: @first_rid,
            state: nil, database: opts[:database]}}
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
    case new_data(tail <> data, %{s | tail: ""}) do
      {:ok, s} ->
        :inet.setopts(socket, active: :once)
        {:noreply, s}
      {:error, error, s} ->
        error(error, s)
    end
  end

  def handle_info({:tcp_closed, _}, s) do
    error(%Mongo.Error{message: "tcp closed"}, s)
  end

  def handle_info({:tcp_error, _, reason}, s) do
    error(%Mongo.Error{message: "tcp error: #{reason}"}, s)
  end

  defp new_data(data, %{tail: tail, state: state} = s) do
    data = tail <> data

    case decode(data) do
      {:ok, id, reply, tail} ->
        s = %{s | tail: tail}
        if :query_failure in op_reply(reply, :flags),
            do: failure(state, id, reply, s),
          else: reply(state, id, reply, s)
      :error ->
        {:ok, %{s | tail: data}}
    end
  end

  defp reply(:nonce, @nonce_rid, op_reply(docs: [%{"nonce" => nonce, "ok" => 1.0}]),
             %{opts: opts} = s) do
    username = Keyword.fetch!(opts, :username)
    password = Keyword.fetch!(opts, :password)
    digest   = digest(nonce, username, password)
    doc      = %{authenticate: 1, user: username, nonce: nonce, key: digest}

    find_one(@auth_rid, "$cmd", doc, nil, s)
    {:ok, %{s | state: :auth}}
  end

  defp reply(:auth, @auth_rid, op_reply(docs: [%{"ok" => 1.0}]), s) do
    {:ok, %{s | state: :ready}}
  end

  defp failure(_state, _id, op_reply(docs: [%{"$err" => reason}]), s) do
    # TODO: reply to the correct place based on id
    {:error, %Mongo.Error{message: reason}, s}
  end

  defp auth(s) do
    find_one(@nonce_rid, "$cmd", %{getnonce: 1}, nil, s)
    %{s | state: :nonce}
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
    :crypto.hash(:md5, [username, <<":mongo:">>, password])
    |> Base.encode16(case: :lower)
  end

  defp namespace(coll, s) do
    if :binary.match(coll, ".") == :nomatch do
      [s.database, ?. | coll]
    else
      coll
    end
  end
end
