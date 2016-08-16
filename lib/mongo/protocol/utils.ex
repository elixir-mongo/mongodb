defmodule Mongo.Protocol.Utils do
  @moduledoc false
  import Kernel, except: [send: 2]
  import Mongo.Messages

  def message(id, ops, s) when is_list(ops) do
    with :ok <- send(ops, s),
         {:ok, ^id, reply} <- recv(s),
         do: {:ok, reply}
  end
  def message(id, op, s) do
    with :ok <- send(id, op, s),
         {:ok, ^id, reply} <- recv(s),
         do: {:ok, reply}
  end

  def command(id, command, s) do
    op = op_query(coll: namespace("$cmd", s), query: BSON.Encoder.document(command),
                  select: "", num_skip: 0, num_return: 1, flags: [])
    case message(id, op, s) do
      {:ok, op_reply(docs: docs)} ->
        case BSON.Decoder.documents(docs) do
          []    -> {:ok, nil}
          [doc] -> {:ok, doc}
        end
      {:disconnect, _, _} = error ->
        error
    end
  end

  def send(id, op, s) do
    case :gen_tcp.send(s.socket, encode(id, op)) do
      :ok              -> :ok
      {:error, reason} -> send_error(reason, s)
    end
  end

  # Performance regressions of a factor of 1000x have been observed on
  # linux systems for write operations that do not include the getLastError
  # command in the same call to :gen_tcp.send/2 so we hide the workaround
  # for mongosniff behind a flag
  if Mix.env in [:dev, :test] && System.get_env("MONGO_NO_BATCH_SEND") do
    def send(ops, s) do
      # Do a separate :gen_tcp.send/2 for each message because mongosniff
      # cannot handle more than one message per packet. TCP is a stream
      # protocol, but no.
      # https://jira.mongodb.org/browse/TOOLS-821
      Enum.find_value(List.wrap(ops), fn {id, op} ->
        data = encode(id, op)
        case :gen_tcp.send(s.socket, data) do
          :ok              -> nil
          {:error, reason} -> send_error(reason, s)
        end
      end)
      || :ok
    end
  else
    def send(ops, s) do
      data =
        Enum.reduce(List.wrap(ops), "", fn {id, op}, acc ->
          [acc|encode(id, op)]
        end)

      case :gen_tcp.send(s.socket, data) do
        :ok              -> :ok
        {:error, reason} -> send_error(reason, s)
      end
    end
  end

  def recv(s) do
    recv(nil, "", s)
  end

  # TODO: Optimize to reduce :gen_tcp.recv and decode_message calls
  #       based on message size in header.
  #       :gen.tcp.recv(socket, min(size, max_packet))
  #       where max_packet = 64mb
  defp recv(nil, data, s) do
    case decode_header(data) do
      {:ok, header, rest} ->
        recv(header, rest, s)
      :error ->
        case :gen_tcp.recv(s.socket, 0, s.timeout) do
          {:ok, tail}      -> recv(nil, [data|tail], s)
          {:error, reason} -> recv_error(reason, s)
        end
    end
  end
  defp recv(header, data, s) do
    case decode_message(header, data) do
      {:ok, id, reply, ""} ->
        {:ok, id, reply}
      :error ->
        case :gen_tcp.recv(s.socket, 0, s.timeout) do
          {:ok, tail}      -> recv(header, [data|tail], s)
          {:error, reason} -> recv_error(reason, s)
        end
    end
  end

  defp send_error(reason, s) do
    error = Mongo.Error.exception(tag: :tcp, action: "send", reason: reason)
    {:disconnect, error, s}
  end

  defp recv_error(reason, s) do
    error = Mongo.Error.exception(tag: :tcp, action: "recv", reason: reason)
    {:disconnect, error, s}
  end

  def namespace(coll, s),
    do: [s.database, ?. | coll]

  def digest(nonce, username, password) do
    :crypto.hash(:md5, [nonce, username, digest_password(username, password)])
    |> Base.encode16(case: :lower)
  end

  def digest_password(username, password) do
    :crypto.hash(:md5, [username, ":mongo:", password])
    |> Base.encode16(case: :lower)
  end

  def assign_ids(doc) when is_map(doc),
    do: [assign_id(doc)] |> unzip
  def assign_ids([{_, _} | _] = doc),
    do: [assign_id(doc)] |> unzip
  def assign_ids(list) when is_list(list),
    do: Enum.map(list, &assign_id/1) |> unzip

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
  defp add_id([{key, _}|_] = list, id) when is_atom(key),
    do: [{:_id, id}|list]
  defp add_id([{key, _}|_] = list, id) when is_binary(key),
    do: [{"_id", id}|list]
  defp add_id([], id),
    do: [{"_id", id}] # Why are you inserting empty documents =(

  # TODO: Enum.unzip ?
  defp unzip(list) do
    {xs, ys} =
      Enum.reduce(list, {[], []}, fn {x, y}, {xs, ys} ->
        {[x|xs], [y|ys]}
      end)

    {Enum.reverse(xs), Enum.reverse(ys)}
  end
end
