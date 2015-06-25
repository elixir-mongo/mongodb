defmodule Mongo.Connection do
  import Kernel, except: [send: 2]
  import Mongo.Protocol
  import Mongo.Connection.Utils
  require Logger
  alias Mongo.Connection.Auth
  alias Mongo.ReadResult
  alias Mongo.WriteResult

  @behaviour Connection
  @requestid_max 2147483648
  @write_concern ~w(w j fsync wtimeout)a
  @insert_flags ~w(continue_on_error)a
  @find_one_flags ~w(slave_ok exhaust partial)a
  @find_flags ~w(tailable_cursor slave_ok no_cursor_timeout await_data exhaust partial)a
  @update_flags ~w(upsert multi)a

  def start_link(opts) do
    Connection.start_link(__MODULE__, opts)
  end

  def stop(conn) do
    Connection.cast(conn, :stop)
  end

  def auth(conn, database, username, password) do
    GenServer.call(conn, {:auth, database, username, password})
  end

  def database(conn) do
    GenServer.call(conn, :database)
  end

  def database(conn, database) do
    GenServer.call(conn, {:database, database})
  end

  def find(conn, coll, query, select, opts \\ []) do
    GenServer.call(conn, {:find, coll, query, select, opts})
  end

  def get_more(conn, coll, cursor_id, opts \\ []) do
    GenServer.call(conn, {:get_more, coll, cursor_id, opts})
  end

  def kill_cursors(conn, cursor_ids) do
    GenServer.call(conn, {:kill_cursors, List.wrap(cursor_ids)})
  end

  def find_one(conn, coll, query, select, opts \\ []) do
    GenServer.call(conn, {:find_one, coll, query, select, opts})
  end

  def insert(conn, coll, docs, opts \\ []) do
    docs = assign_ids(docs)
    GenServer.call(conn, {:insert, coll, docs, opts})
  end

  def update(conn, coll, query, update, opts \\ []) do
    GenServer.call(conn, {:update, coll, query, update, opts})
  end

  def remove(conn, coll, query, opts \\ []) do
    GenServer.call(conn, {:remove , coll, query, opts})
  end

  def wire_version(conn) do
    GenServer.call(conn, :wire_version)
  end

  defp assign_ids(doc) when is_map(doc) do
    assign_id(doc)
  end

  defp assign_ids([{_, _} | _] = doc) do
    assign_id(doc)
  end

  defp assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
  end

  defp assign_id(%{_id: value} = map) when value != nil,
    do: map
  defp assign_id(%{"_id" => value} = map) when value != nil,
    do: map

  defp assign_id([{_, _} | _] = keyword) do
    case Keyword.take(keyword, [:_id, "_id"]) do
      [{_key, value} | _] when value != nil ->
        keyword
      [] ->
        add_id(keyword)
    end
  end

  defp assign_id(map) when is_map(map) do
    map |> Map.to_list |> add_id
  end

  defp add_id(doc) do
    add_id(doc, Mongo.IdServer.new)
  end
  defp add_id([{key, _}|_] = list, id) when is_atom(key) do
    [{:_id, id}|list]
  end
  defp add_id([{key, _}|_] = list, id) when is_binary(key) do
    [{"_id", id}|list]
  end
  defp add_id([], id) do
    # Why are you inserting empty documents =(
    %{"_id" => id}
  end

  @doc false
  def init(opts) do
    :random.seed(:os.timestamp)
    timeout = opts[:timeout] || 5000

    opts = opts
           |> Keyword.put_new(:hostname, "localhost")
           |> Keyword.update!(:hostname, &to_char_list/1)
           |> Keyword.put_new(:port, 27017)
           |> Keyword.put_new(:backoff, 1000)
           |> Keyword.delete(:timeout)

    {write_concern, opts} = Keyword.split(opts, @write_concern)
    write_concern = Keyword.put_new(write_concern, :w, 1)

    s = %{socket: nil, auth: nil, tail: nil, queue: %{}, request_id: 0, opts: opts,
          database: nil, timeout: timeout, write_concern: write_concern,
          wire_version: nil}

    s = Auth.setup(s)
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
        s = %{s | socket: socket, tail: ""}
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])
        buffer = buffer |> max(sndbuf) |> max(recbuf)
        :ok = :inet.setopts(socket, buffer: buffer)

        case init_connection(s) do
          {:ok, s} ->
            :ok = :inet.setopts(socket, active: :once)
            connect_hook(s)
            {:ok, s}
          {:error, reason} ->
            {:stop, reason, s}
          {:tcp_error, reason} ->
            Logger.error "Mongo tcp error (#{host}:#{port}): #{format_error(reason)}"
            {:backoff, s.opts[:backoff], s}
        end

      {:error, reason} ->
        Logger.error "Mongo connect error (#{host}:#{port}): #{format_error(reason)}"
        {:backoff, s.opts[:backoff], s}
    end
  end

  defp init_connection(s) do
    # wire version
    # https://github.com/mongodb/mongo/blob/master/src/mongo/db/wire_version.h
    case sync_command(-1, s.database, [ismaster: 1], s) do
      {:ok, %{"ok" => 1.0} = reply} ->
        s = %{s | wire_version: reply["maxWireVersion"] || 0}
        Auth.run(s)
      {:tcp_error, _} = error ->
        error
    end
  end

  defp connect_hook(s) do
    if pid = s.opts[:on_connect] do
      Kernel.send(pid, {__MODULE__, :on_connect, self})
    end
  end

  @doc false
  def disconnect({:error, reason}, s) do
    formatted_reason = format_error(reason)

    Enum.each(s.queue, fn
      {_id, %{from: nil}} ->
        :ok
      {_id, %{from: from}} ->
        error = %Mongo.Error{message: "Mongo tcp error: #{formatted_reason}"}
        reply(from, {:error, error})
    end)

    host = s.opts[:hostname]
    port = s.opts[:port] || 27017
    Logger.error "Mongo tcp error (#{host}:#{port}): #{formatted_reason}"

    # Backoff 0 to churn through all commands in mailbox before reconnecting
    {:backoff, 0, %{s | socket: nil, queue: %{}}}
  end

  def disconnect(:close, %{socket: nil} = s) do
    {:stop, :normal, s}
  end

  def disconnect(:close, %{socket: socket} = s) do
    :gen_tcp.close(socket)
    {:stop, :normal, %{s | socket: nil}}
  end

  @doc false
  def handle_cast(:stop, s) do
    {:disconnect, :close, s}
  end

  def handle_cast(msg, _) do
    exit({:bad_cast, msg})
  end

  @doc false
  def handle_call(_, _from, %{socket: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:auth, database, username, password}, from, s) do
    {id, s} = new_command(:nonce, {database, username, password}, from, s)
    op_query(coll: namespace({database, "$cmd"}, s), query: [getnonce: 1],
             select: nil, num_skip: 0, num_return: 1, flags: [])
    |> send(id, s)
    |> send_to_noreply
  end

  def handle_call(:database, _from, %{database: database} = s) do
    {:reply, database, s}
  end

  def handle_call({:database, database}, _from, s) do
    {:reply, :ok, %{s | database: database}}
  end

  def handle_call({:find, coll, query, select, opts}, from, s) do
    flags      = Keyword.take(opts, @find_flags)
    num_skip   = Keyword.get(opts, :num_skip, 0)
    num_return = Keyword.get(opts, :num_return, 0)
    {id, s}    = new_command(:find, nil, from, s)

    op_query(coll: namespace(coll, s), query: query, select: select,
             num_skip: num_skip, num_return: num_return, flags: flags(flags))
    |> send(id, s)
    |> send_to_noreply
  end

  def handle_call({:get_more, coll, cursor_id, opts}, from, s) do
    num_return = Keyword.get(opts, :num_return, 0)
    {id, s}    = new_command(:get_more, nil, from, s)

    op_get_more(coll: namespace(coll, s), cursor_id: cursor_id,
                num_return: num_return)
    |> send(id, s)
    |> send_to_noreply
  end

  def handle_call({:kill_cursors, cursor_ids}, _from, s) do
    op_kill_cursors(cursor_ids: cursor_ids)
    |> send(-11, s)
    |> send_to_reply(:ok)
  end

  def handle_call({:find_one, coll, query, select, opts}, from, s) do
    flags   = Keyword.take(opts, @find_one_flags)
    {id, s} = new_command(:find_one, nil, from, s)

    op_query(coll: namespace(coll, s), query: query, select: select,
             num_skip: 0, num_return: 1, flags: flags(flags))
    |> send(id, s)
    |> send_to_noreply
  end

  def handle_call({:insert, coll, docs, opts}, from, s) do
    flags = Keyword.take(opts, @insert_flags)
    docs = doc_wrap(docs)
    insert_op = {-10, op_insert(coll: namespace(coll, s), docs: docs, flags: flags(flags))}

    if s.write_concern[:w] == 0 do
      insert_op |> send(s) |> send_to_reply(:ok)
    else
      params = %{type: :insert, n: length(docs)}
      {get_last_error, s} = get_last_error(coll, params, from, s)

      [insert_op, get_last_error]
      |> send(s)
      |> send_to_noreply
    end
  end

  def handle_call({:update, coll, query, update, opts}, from, s) do
    flags = Keyword.take(opts, @update_flags)
    update_op = {-12, op_update(coll: namespace(coll, s), query: query,
                                update: update, flags: flags(flags))}

    params = %{type: :update}
    {get_last_error, s} = get_last_error(coll, params, from, s)

    [update_op, get_last_error]
    |> send(s)
    |> send_to_noreply
  end

  def handle_call({:remove, coll, query, opts}, from, s) do
    flags = if Keyword.get(opts, :multi, false), do: [], else: [:single]
    delete_op = {-13, op_delete(coll: namespace(coll, s), query: query,
                                flags: flags)}

    params = %{type: :remove}
    {get_last_error, s} = get_last_error(coll, params, from, s)

    [delete_op, get_last_error]
    |> send(s)
    |> send_to_noreply
  end

  def handle_call(:wire_version, _from, s) do
    {:reply, s.wire_version, s}
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
    # NOTE: This can be optimized by building an iolist and only concat to
    #       a binary when we know we have enough data according to the
    #       message header.
    data = tail <> data

    # Also note that this is re-parsing the message header every time
    # on new data
    case decode(data) do
      {:ok, id, reply, tail} ->
        command = s.queue[id][:command]
        s = %{s | tail: tail}

        if :query_failure in op_reply(reply, :flags),
            do: failure(command, id, reply, s),
          else: message(command, id, reply, s)

      :error ->
        {:ok, %{s | tail: data}}
    end
  end

  defp message(:nonce, id, op_reply(docs: [%{"nonce" => nonce, "ok" => 1.0}]), s) do
    {database, username, password} = s.queue[id].params
    digest = digest(nonce, username, password)
    doc = %{authenticate: 1, user: username, nonce: nonce, key: digest}
    s = command(id, :auth, s)

    op_query(coll: namespace({database, "$cmd"}, s), query: doc, select: nil,
             num_skip: 0, num_return: 1, flags: [])
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
    {:ok, reply(id, %Mongo.Error{message: "authentication failed: #{reason}", code: code}, s)}
  end

  defp message(:auth, id, op_reply(docs: []), s) do
    {:ok, reply(id, %Mongo.Error{message: "authentication failed"}, s)}
  end

  defp message(:find, id, op_reply(docs: docs, cursor_id: cursor_id, from: from, num: num), s) do
    result = %ReadResult{from: from, num: num, cursor_id: cursor_id, docs: docs}
    {:ok, reply(id, {:ok, result}, s)}
  end

  defp message(:get_more, id, op_reply(flags: flags, docs: docs, cursor_id: cursor_id, from: from, num: num), s) do
    if :cursor_not_found in flags do
      reason = %Mongo.Error{message: "cursor not found"}
      {:ok, reply(id, {:error, reason}, s)}
    else
      result = %ReadResult{from: from, num: num, cursor_id: cursor_id, docs: docs}
      {:ok, reply(id, {:ok, result}, s)}
    end
  end

  defp message(:find_one, id, op_reply(docs: docs), s) do
    case docs do
      [doc] -> {:ok, reply(id, doc, s)}
      []    -> {:ok, reply(id, nil, s)}
    end
  end

  defp message(:get_last_error, id, op_reply(docs: [doc]), s) do
    case doc do
      %{"ok" => 1.0, "err" => nil} ->
        params = s.queue[id].params
        result = write_result(params, doc)
        s = reply(id, {:ok, result}, s)

      %{"ok" => 1.0, "err" => message, "code" => code} ->
        s = reply(id, {:error, %Mongo.Error{message: message, code: code}}, s)

      %{"ok" => 0.0, "errmsg" => message, "code" => code} ->
        s = reply(id, {:error, %Mongo.Error{message: message, code: code}}, s)
    end

    {:ok, s}
  end

  defp write_result(%{type: :insert, n: n}, _doc),
    do: %WriteResult{type: :insert, num_inserted: n}
  defp write_result(%{type: :update}, %{"n" => n, "upserted" => id}),
    do: %WriteResult{type: :update, num_matched: n, upserted_id: id}
  defp write_result(%{type: :update}, %{"n" => n, }),
    do: %WriteResult{type: :update, num_matched: n}
  defp write_result(%{type: :remove}, %{"n" => n}),
    do: %WriteResult{type: :remove, num_matched: n, num_removed: n}

  defp failure(_command, id, op_reply(docs: [%{"$err" => reason, "code" => code }]), s) do
    {:ok, reply(id, %Mongo.Error{message: reason, code: code}, s)}
  end

  defp get_last_error(coll, params, from, s) do
    {id, s} = new_command(:get_last_error, params, from, s)
    command = [{:getLastError, 1}|s.write_concern]
    op = op_query(coll: namespace({:override, coll, "$cmd"}, s), query: command,
                  select: nil, num_skip: 0, num_return: 1, flags: [])

    {{id, op}, s}
  end

  defp new_command(command, params, from, s) do
    command    = %{command: command, params: params, from: from}
    queue      = Map.put(s.queue, s.request_id, command)
    request_id = rem s.request_id+1, @requestid_max

    {s.request_id, %{s | request_id: request_id, queue: queue}}
  end

  defp command(id, command, s) do
    put_in(s.queue[id].command, command)
  end

  defp reply(id, reply, s) do
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

  defp flags(flags) do
    Enum.reduce(flags, [], fn
      {flag, true},   acc -> [flag|acc]
      {_flag, false}, acc -> acc
    end)
  end

  defp doc_wrap(%{} = doc),
    do: [doc]
  defp doc_wrap([{_, _}|_] = doc),
    do: [doc]
  defp doc_wrap(list) when is_list(list),
    do: list
end
