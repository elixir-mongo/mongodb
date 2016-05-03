defmodule Mongo.Protocol do
  use DBConnection
  use Bitwise
  use Mongo.Messages
  alias Mongo.Protocol.Utils
  alias Mongo.ReadResult
  alias Mongo.WriteResult

  @timeout 5000
  @find_flags ~w(tailable_cursor slave_ok no_cursor_timeout await_data exhaust allow_partial_results)a
  @insert_flags ~w(continue_on_error)a
  @write_concern ~w(w j wtimeout)a

  def connect(opts) do
    {write_concern, opts} = Keyword.split(opts, @write_concern)
    write_concern = Keyword.put_new(write_concern, :w, 1)

    s = %{socket: nil,
          request_id: 0,
          timeout: opts[:timeout] || @timeout,
          database: Keyword.fetch!(opts, :database),
          write_concern: write_concern,
          wire_version: nil}

    connect(opts, s)
  end

  defp connect(opts, s) do
    with {:ok, s} <- tcp_connect(opts, s),
         {:ok, s} <- wire_version(s),
         {:ok, s} <- Mongo.Auth.run(opts, s) do
      :ok = :inet.setopts(s.socket, active: :once)
      Mongo.Monitor.add_conn(self, opts[:name], s.wire_version)
      {:ok, s}
    else
      {:disconnect, {:tcp_recv, reason}, _s} ->
        {:error, Mongo.Error.exception(tag: :tcp, action: "recv", reason: reason)}
      {:disconnect, {:tcp_send, reason}, _s} ->
        {:error, Mongo.Error.exception(tag: :tcp, action: "send", reason: reason)}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp tcp_connect(opts, s) do
    host      = (opts[:hostname] || "localhost") |> to_char_list
    port      = opts[:port] || 27017
    sock_opts = [:binary, active: false, packet: :raw, send_timeout: s.timeout, nodelay: true]
                ++ (opts[:socket_options] || [])

    case :gen_tcp.connect(host, port, sock_opts, s.timeout) do
      {:ok, socket} ->
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])
        buffer = buffer |> max(sndbuf) |> max(recbuf)
        :ok = :inet.setopts(socket, buffer: buffer)

        {:ok, %{s | socket: socket}}

      {:error, reason} ->
        {:error, Mongo.Error.exception(tag: :tcp, action: "connect", reason: reason)}
    end
  end

  defp wire_version(s) do
    # wire version
    # https://github.com/mongodb/mongo/blob/master/src/mongo/db/wire_version.h
    case Utils.command(-1, [ismaster: 1], s) do
      {:ok, %{"ok" => 1.0, "maxWireVersion" => version}} ->
        {:ok, %{s | wire_version: version || 0}}
      {:disconnect, _, _} = error ->
        error
    end
  end

  def handle_info({:tcp, data}, s) do
    err = Postgrex.Error.exception(message: "unexpected async recv: #{inspect data}")
    {:disconnect, err, s}
  end

  def handle_info({:tcp_closed, _}, s) do
    err = Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: :closed)
    {:disconnect, err, s}
  end

  def handle_info({:tcp_error, _, reason}, s) do
    err = Postgrex.Error.exception(tag: :tcp, action: "async recv", reason: reason)
    {:disconnect, err, s}
  end

  def checkout(s) do
    :ok = :inet.setopts(s.socket, [active: false])
    {:ok, s}
  end

  def checkin(s) do
    :ok = :inet.setopts(s.socket, [active: :once])
    {:ok, s}
  end

  def handle_execute_close(query, params, opts, s) do
    handle_execute(query, params, opts, s)
  end

  def handle_execute(%Mongo.Query{action: action}, [], opts, s) do
    handle_execute(action, opts, s)
  end

  defp handle_execute({:find, coll, query, select}, opts, s) do
    flags      = Keyword.take(opts, @find_flags)
    num_skip   = Keyword.get(opts, :skip, 0)
    num_return = Keyword.get(opts, :batch_size, 0)

    op_query(coll: Utils.namespace(coll, s), query: query, select: select,
             num_skip: num_skip, num_return: num_return, flags: flags(flags))
    |> message_reply(s, &find_reply/2)
  end

  defp handle_execute({:get_more, coll, cursor_id}, opts, s) do
    num_return = Keyword.get(opts, :batch_size, 0)

    op_get_more(coll: Utils.namespace(coll, s), cursor_id: cursor_id,
                num_return: num_return)
    |> message_reply(s, &get_more/2)
  end

  defp handle_execute({:kill_cursors, cursor_ids}, _opts, s) do
    op = op_kill_cursors(cursor_ids: cursor_ids)
    with :ok <- Utils.send(-10, op, s),
         do: {:ok, :ok, s}
  end

  defp handle_execute({:insert, coll, docs}, opts, s) do
    {ids, docs} = assign_ids(docs)
    flags  = Keyword.take(opts, @insert_flags)
    op     = op_insert(coll: Utils.namespace(coll, s), docs: docs, flags: flags(flags))
    params = %{type: :insert, n: length(docs), ids: ids}
    message_gle(-11, op, params, opts, s)
  end

  defp find_reply(op_reply(docs: docs, cursor_id: cursor_id, from: from, num: num), s) do
    result = %ReadResult{from: from, num: num, cursor_id: cursor_id, docs: docs}
    {:ok, result, s}
  end

  def get_more(op_reply(flags: flags, docs: docs, cursor_id: cursor_id, from: from, num: num), s) do
    if @reply_cursor_not_found &&& flags != 0 do
      error = Mongo.Error.exception(message: "cursor not found: #{inspect cursor_id}")
      {:error, error, s}
    else
      result = %ReadResult{from: from, num: num, cursor_id: cursor_id, docs: docs}
      {:ok, result, s}
    end
  end

  defp message_reply(op, s, fun) do
    with {:ok, reply} <- Utils.message(s.request_id, op, s),
         s = %{s | request_id: s.request_id + 1},
         :ok <- maybe_failure(reply, s),
         do: fun.(reply, s)
  end

  defp flags(flags) do
    Enum.reduce(flags, [], fn
      {flag, true},   acc -> [flag|acc]
      {_flag, false}, acc -> acc
    end)
  end

  defp message_gle(id, op, params, opts, s) do
    write_concern = Keyword.take(opts, @write_concern)
    write_concern = Dict.merge(s.write_concern, write_concern)

    if write_concern[:w] == 0 do
      with :ok <- Utils.send(id, op, s),
           do: {:ok, :ok, s}
    else
      command = [{:getLastError, 1}|write_concern]
      gle_op = op_query(coll: Utils.namespace("$cmd", s), query: command,
                        select: nil, num_skip: 0, num_return: -1, flags: [])

      ops = [{id, op}, {s.request_id, gle_op}]
      message_reply(ops, s, &get_last_error(&1, params, &2))
    end
  end

  defp get_last_error(op_reply(docs: [%{"ok" => 1.0, "err" => nil} = doc]), params, s) do
    result = write_result(params, doc)
    {:ok, result, s}
  end
  defp get_last_error(op_reply(docs: [%{"ok" => 1.0, "err" => message, "code" => code}]), _params, s) do
    # If a batch insert (OP_INSERT) fails some documents may still have been
    # inserted, but mongo always returns {n: 0}
    # When we support the 2.6 bulk write API we will get number of inserted
    # documents and should change the return value to be something like:
    # {:error, %WriteResult{}, %Error{}}
    {:error, Mongo.Error.exception(message: message, code: code), s}
  end
  defp get_last_error(op_reply(docs: [%{"ok" => 0.0, "errmsg" => message, "code" => code}]), _params, s) do
    {:error, Mongo.Error.exception(message: message, code: code), s}
  end

  defp write_result(%{type: :insert, n: n, ids: ids}, _doc),
    do: %WriteResult{type: :insert, num_inserted: n, inserted_ids: ids}
  defp write_result(%{type: :update}, %{"n" => 1, "upserted" => id}),
    do: %WriteResult{type: :update, num_matched: 0, num_modified: 1, upserted_id: id}
  defp write_result(%{type: :update}, %{"n" => n}),
    do: %WriteResult{type: :update, num_matched: n, num_modified: n}
  defp write_result(%{type: :remove}, %{"n" => n}),
    do: %WriteResult{type: :remove, num_matched: n, num_removed: n}

  defp maybe_failure(op_reply(flags: flags, docs: [%{"$err" => reason, "code" => code}]), s)
    when @reply_query_failure &&& flags != 0,
    do: {:error, Mongo.Error.exception(message: reason, code: code), s}
  defp maybe_failure(_reply, _s),
    do: :ok


  defp assign_ids(doc) when is_map(doc) do
    [assign_id(doc)]
    |> Enum.unzip
  end

  defp assign_ids([{_, _} | _] = doc) do
    [assign_id(doc)]
    |> Enum.unzip
  end

  defp assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
    |> Enum.unzip
  end
  defp assign_id(%{_id: id} = map) when id != nil,
    do: {id, map}
  defp assign_id(%{"_id" => id} = map) when id != nil,
    do: {id, map}

  defp assign_id([{_, _} | _] = keyword) do
    case Keyword.take(keyword, [:_id, "_id"]) do
      [{_key, id} | _] when id != nil ->
        {id, keyword}
      [] ->
        add_id(keyword)
    end
  end

  defp assign_id(map) when is_map(map) do
    map |> Map.to_list |> add_id
  end

  defp add_id(doc) do
    id = Mongo.IdServer.new
    {id, add_id(doc, id)}
  end
  defp add_id([{key, _}|_] = list, id) when is_atom(key) do
    [{:_id, id}|list]
  end
  defp add_id([{key, _}|_] = list, id) when is_binary(key) do
    [{"_id", id}|list]
  end
  defp add_id([], id) do
    # Why are you inserting empty documents =(
    [{"_id", id}]
  end
end
