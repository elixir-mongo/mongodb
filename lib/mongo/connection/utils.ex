defmodule Mongo.Connection.Utils do
  @moduledoc false
  import Mongo.Protocol

  def sync_command(id, database, command, s) do
    op = op_query(coll: namespace({database, "$cmd"}, s), query: command,
                  select: nil, num_skip: 0, num_return: 1, flags: [])
    case send(op, id, s) do
      {:ok, s} ->
        case sync_recv(s) do
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

  def send(op, id, s) do
    data = encode(id, op)
    case :gen_tcp.send(s.socket, data) do
      :ok ->
        {:ok, s}
      {:error, _} = error ->
        error
    end
  end

  def send(ops, s) do
    ops = List.wrap(ops)

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

  # TODO: Fix the terrible :override hack
  def namespace({:override, {database, _}, coll}, _s),
    do: [database, ?. | coll]
  def namespace({:override, _, coll}, s),
    do: [s.database, ?. | coll]
  def namespace({:override, coll}, _s),
    do: coll
  def namespace({database, coll}, _s),
    do: [database, ?. | coll]
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

  defp sync_recv(tail \\ "", s) do
    case :gen_tcp.recv(s.socket, 0, s.timeout) do
      {:ok, data} ->
        data = tail <> data
        case decode(data) do
          {:ok, id, reply, ""} ->
            {:ok, id, reply}
          :error ->
            sync_recv(data, s)
        end

      {:error, reason} ->
        {:tcp_error, reason}
    end
  end
end
