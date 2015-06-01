defmodule Mongo.Connection do
  import Kernel, except: [send: 2]
  import Mongo.Protocol
  import Mongo.Connection.Utils
  require Logger
  alias Mongo.Connection.Auth

  @behaviour Connection
  @backoff 1000
  @timeout 5000
  @requestid_max 2147483648
  @write_concern ~w(w j fsync wtimeout)a
  @insert_flags ~w(continue_on_error)a
  @find_one_flags ~w(slave_ok exhaust partial)a
  @find_flags ~w(tailable_cursor slave_ok no_cursor_timeout await_data exhaust partial)a
  @update_flags ~w(upsert multi)a

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
        s = %{s | socket: socket}

        case Auth.init(s) do
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
    op_query(coll: namespace({database, "$cmd"}, s), query: %{getnonce: 1},
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
    {id, s}    = new_command(:find_all, nil, from, s)

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
    insert_op = {-10, op_insert(coll: namespace(coll, s), docs: List.wrap(docs),
                                flags: flags(flags))}

    if s.write_concern.w == 0 do
      insert_op |> send(s) |> send_to_reply(:ok)
    else
      {get_last_error, s} = get_last_error(coll, from, s)

      [insert_op, get_last_error]
      |> send(s)
      |> send_to_noreply
    end
  end

  def handle_call({:update, coll, query, update, opts}, from, s) do
    flags = Keyword.take(opts, @update_flags)
    update_op = {-12, op_update(coll: namespace(coll, s), query: query,
                                update: update, flags: flags(flags))}

    {get_last_error, s} = get_last_error(coll, from, s)

    [update_op, get_last_error]
    |> send(s)
    |> send_to_noreply
  end

  def handle_call({:delete, coll, query, opts}, from, s) do
    flags = if Keyword.get(opts, :multi, false), do: [], else: [:single]
    delete_op = {-13, op_delete(coll: namespace(coll, s), query: query,
                                flags: flags)}

    {get_last_error, s} = get_last_error(coll, from, s)

    [delete_op, get_last_error]
    |> send(s)
    |> send_to_noreply
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

  defp message(:find_all, id, op_reply(docs: docs, cursor_id: cursor_id), s) do
    {:ok, reply(id, {:ok, cursor_id, docs}, s)}
  end

  defp message(:get_more, id, op_reply(flags: flags, docs: docs, cursor_id: cursor_id), s) do
    if :cursor_not_found in flags do
      reason = %Mongo.Error{message: "cursor not found"}
      {:ok, reply(id, {:error, reason}, s)}
    else
      {:ok, reply(id, {:ok, cursor_id, docs}, s)}
    end
  end

  defp message(:find_one, id, op_reply(docs: docs), s) do
    case docs do
      [doc] -> {:ok, reply(id, doc, s)}
      []    -> {:ok, reply(id, nil, s)}
    end
  end

  defp message(:get_last_error, id, op_reply(docs: docs), s) do
    case docs do
      [%{"ok" => 1.0}] ->
        s = reply(id, :ok, s)
      [%{"ok" => 0.0, "err" => reason, "code" => code}] ->
        s = reply(id, {:error, %Mongo.Error{message: reason, code: code}}, s)
    end
    {:ok, s}
  end

  defp failure(_command, id, op_reply(docs: [%{"$err" => reason, "code" => code }]), s) do
    {:ok, reply(id, %Mongo.Error{message: reason, code: code}, s)}
  end

  defp get_last_error(coll, from, s) do
    {id, s} = new_command(:get_last_error, nil, from, s)
    command = Map.merge(%{getLastError: 1}, s.write_concern)
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
end
