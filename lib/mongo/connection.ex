defmodule Mongo.Connection do
  @moduledoc """
  A connection process to a MongoDB server.
  """

  import Kernel, except: [send: 2]
  import Mongo.Protocol
  import Mongo.Connection.Utils
  require Logger
  alias Mongo.Connection.Auth
  alias Mongo.ReadResult
  alias Mongo.WriteResult

  @behaviour Connection
  @requestid_max 2147483648
  @write_concern ~w(w j wtimeout)a
  @insert_flags ~w(continue_on_error)a
  @find_one_flags ~w(slave_ok exhaust partial)a
  @find_flags ~w(tailable_cursor slave_ok no_cursor_timeout await_data exhaust allow_partial_results)a
  @update_flags ~w(upsert multi)a

  @doc """
  Starts the connection process.

  ## Options

    * `:hostname` - Server hostname (Default: "localhost")
    * `:port` - Server port (Default: 27017)
    * `:database` - Database (required);
    * `:username` - Username
    * `:password` - User password
    * `:backoff` - Backoff time for reconnects, the first reconnect is
      instantaneous (Default: 1000)
    * `:timeout` - TCP connect and receive timeouts (Default: 5000)
    * `:w` - The number of servers to replicate to before returning from write
      operators, a 0 value will return immediately, :majority will wait until
      the operation propagates to a majority of members in the replica set
      (Default: 1)
    * `:j` If true, the write operation will only return after it has been
      committed to journal - (Default: false)
    * `:wtimeout` - If the write concern is not satisfied in the specified
      interval, the operation returns an error
  """
  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts) do
    Connection.start_link(__MODULE__, opts)
  end

  @doc """
  Stops the connection process.
  """
  @spec stop(pid) :: :ok
  def stop(conn) do
    Connection.cast(conn, :stop)
  end

  @doc false
  def find(conn, coll, query, select, opts \\ []) do
    GenServer.call(conn, {:find, coll, query, select, opts})
  end

  @doc false
  def get_more(conn, coll, cursor_id, opts \\ []) do
    GenServer.call(conn, {:get_more, coll, cursor_id, opts})
  end

  @doc false
  def kill_cursors(conn, cursor_ids) do
    GenServer.call(conn, {:kill_cursors, List.wrap(cursor_ids)})
  end

  @doc false
  def find_one(conn, coll, query, select, opts \\ []) do
    GenServer.call(conn, {:find_one, coll, query, select, opts})
  end

  @doc false
  def insert(conn, coll, docs, opts \\ []) do
    {ids, docs} = assign_ids(docs)
    case GenServer.call(conn, {:insert, coll, docs, opts}) do
      {:ok, result} -> {:ok, %{result | inserted_ids: ids}}
      other -> other
    end
  end

  @doc false
  def update(conn, coll, query, update, opts \\ []) do
    GenServer.call(conn, {:update, coll, query, update, opts})
  end

  @doc false
  def remove(conn, coll, query, opts \\ []) do
    GenServer.call(conn, {:remove , coll, query, opts})
  end

  @doc false
  def wire_version(conn) do
    GenServer.call(conn, :wire_version)
  end

  defp assign_ids(doc) when is_map(doc) do
    [assign_id(doc)]
    |> unzip
  end

  defp assign_ids([{_, _} | _] = doc) do
    [assign_id(doc)]
    |> unzip
  end

  defp assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
    |> unzip
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

  defp unzip(list) do
    {xs, ys} =
      Enum.reduce(list, {[], []}, fn {x, y}, {xs, ys} ->
        {[x|xs], [y|ys]}
      end)

    {Enum.reverse(xs), Enum.reverse(ys)}
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
    sock_opts = [:binary, active: false, packet: :raw, send_timeout: s.timeout, nodelay: true]
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
    case sync_command(-1, [ismaster: 1], s) do
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
        reply({:error, error}, from)
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

  def handle_call({:find, coll, query, select, opts}, from, s) do
    flags      = Keyword.take(opts, @find_flags)
    num_skip   = Keyword.get(opts, :skip, 0)
    num_return = Keyword.get(opts, :batch_size, 0)
    {id, s}    = new_command(:find, nil, from, s)

    op_query(coll: namespace(coll, s), query: query, select: select,
             num_skip: num_skip, num_return: num_return, flags: flags(flags))
    |> send(id, s)
    |> send_to_noreply
  end

  def handle_call({:get_more, coll, cursor_id, opts}, from, s) do
    num_return = Keyword.get(opts, :batch_size, 0)
    {id, s}    = new_command(:get_more, nil, from, s)

    op_get_more(coll: namespace(coll, s), cursor_id: cursor_id,
                num_return: num_return)
    |> send(id, s)
    |> send_to_noreply
  end

  def handle_call({:kill_cursors, cursor_ids}, _from, s) do
    op_kill_cursors(cursor_ids: cursor_ids)
    |> send(-10, s)
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
    insert_op = {-11, op_insert(coll: namespace(coll, s), docs: docs, flags: flags(flags))}

    params = %{type: :insert, n: length(docs)}
    write_concern_op(insert_op, params, opts, from, s)
  end

  def handle_call({:update, coll, query, update, opts}, from, s) do
    flags = Keyword.take(opts, @update_flags)
    update_op = {-12, op_update(coll: namespace(coll, s), query: query,
                                update: update, flags: flags(flags))}

    params = %{type: :update}
    write_concern_op(update_op, params, opts, from, s)
  end

  def handle_call({:remove, coll, query, opts}, from, s) do
    flags = if Keyword.get(opts, :multi, false), do: [], else: [:single]
    delete_op = {-13, op_delete(coll: namespace(coll, s), query: query,
                                flags: flags)}

    params = %{type: :remove}
    write_concern_op(delete_op, params, opts, from, s)
  end

  def handle_call(:wire_version, _from, s) do
    {:reply, s.wire_version, s}
  end

  @doc false
  def handle_info({:tcp, _, data}, %{socket: socket} = s) do
    s = new_data(data, s)
    :inet.setopts(socket, active: :once)
    {:noreply, s}
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
        command = s.queue[id].command
        s = %{s | tail: tail}

        if :query_failure in op_reply(reply, :flags),
            do: failure(command, id, reply, s),
          else: message(command, id, reply, s)

      :error ->
        %{s | tail: data}
    end
  end

  defp message(:find, id, op_reply(docs: docs, cursor_id: cursor_id, from: from, num: num), s) do
    result = %ReadResult{from: from, num: num, cursor_id: cursor_id, docs: docs}
    reply(id, {:ok, result}, s)
  end

  defp message(:get_more, id, op_reply(flags: flags, docs: docs, cursor_id: cursor_id, from: from, num: num), s) do
    if :cursor_not_found in flags do
      reason = %Mongo.Error{message: "cursor not found"}
      reply(id, {:error, reason}, s)
    else
      result = %ReadResult{from: from, num: num, cursor_id: cursor_id, docs: docs}
      reply(id, {:ok, result}, s)
    end
  end

  defp message(:find_one, id, op_reply(docs: docs), s) do
    case docs do
      [doc] -> reply(id, doc, s)
      []    -> reply(id, nil, s)
    end
  end

  defp message(:get_last_error, id, op_reply(docs: [doc]), s) do
    case doc do
      %{"ok" => 1.0, "err" => nil} ->
        params = s.queue[id].params
        result = write_result(params, doc)
        reply(id, {:ok, result}, s)

      %{"ok" => 1.0, "err" => message, "code" => code} ->
        # If a batch insert (OP_INSERT) fails some documents may still have been
        # inserted, but mongo always returns {n: 0}
        # When we support the 2.6 bulk write API we will get number of inserted
        # documents and should change the return value to be something like:
        # {:error, %WriteResult{}, %Error{}}
        reply(id, {:error, %Mongo.Error{message: message, code: code}}, s)

      %{"ok" => 0.0, "errmsg" => message, "code" => code} ->
        reply(id, {:error, %Mongo.Error{message: message, code: code}}, s)
    end
  end

  defp write_result(%{type: :insert, n: n}, _doc),
    do: %WriteResult{type: :insert, num_inserted: n}
  defp write_result(%{type: :update}, %{"n" => 1, "upserted" => id}),
    do: %WriteResult{type: :update, num_matched: 0, num_modified: 1, upserted_id: id}
  defp write_result(%{type: :update}, %{"n" => n}),
    do: %WriteResult{type: :update, num_matched: n, num_modified: n}
  defp write_result(%{type: :remove}, %{"n" => n}),
    do: %WriteResult{type: :remove, num_matched: n, num_removed: n}

  defp failure(_command, id, op_reply(docs: [%{"$err" => reason, "code" => code }]), s) do
    reply(id, %Mongo.Error{message: reason, code: code}, s)
  end

  defp write_concern_op(op, params, opts, from, s) do
    write_concern = Keyword.take(opts, @write_concern)
    write_concern = Dict.merge(s.write_concern, write_concern)

    if write_concern[:w] == 0 do
      op |> send(s) |> send_to_reply(:ok)
    else
      {get_last_error, s} = get_last_error(params, write_concern, from, s)

      [op, get_last_error]
      |> send(s)
      |> send_to_noreply
    end
  end

  defp get_last_error(params, write_concern, from, s) do
    {id, s} = new_command(:get_last_error, params, from, s)
    command = [{:getLastError, 1}|write_concern]
    op = op_query(coll: namespace("$cmd", s), query: command,
                  select: nil, num_skip: 0, num_return: -1, flags: [])

    {{id, op}, s}
  end

  defp new_command(command, params, from, s) do
    command    = %{command: command, params: params, from: from}
    queue      = Map.put(s.queue, s.request_id, command)
    request_id = rem s.request_id+1, @requestid_max

    {s.request_id, %{s | request_id: request_id, queue: queue}}
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
