defmodule Mongo.Connection.Utils do
  @moduledoc false
  import Mongo.Protocol

  def sync_command(id, command, s) do
    op = op_query(coll: namespace("$cmd", s), query: command,
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
          :ok ->
            nil
          {:error, _} = error ->
            error
        end
      end)
      || {:ok, s}
    end

  else

    def send(ops, s) do
      data =
        Enum.reduce(List.wrap(ops), "", fn {id, op}, acc ->
          [acc|encode(id, op)]
        end)

      case :gen_tcp.send(s.socket, data) do
        :ok ->
          {:ok, s}
        {:error, _} = error ->
          error
      end
    end
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
