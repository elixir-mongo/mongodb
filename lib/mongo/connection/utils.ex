defmodule Mongo.Connection.Utils do
  @moduledoc false
  import Mongo.Protocol

  def digest(nonce, username, password) do
    :crypto.hash(:md5, [nonce, username, digest_password(username, password)])
    |> Base.encode16(case: :lower)
  end

  def digest_password(username, password) do
    :crypto.hash(:md5, [username, ":mongo:", password])
    |> Base.encode16(case: :lower)
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
  def namespace({database, coll}, _s),
    do: [database, ?. | coll]
  def namespace(coll, s),
    do: [s.database, ?. | coll]
end
