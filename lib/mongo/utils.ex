defmodule Mongo.Utils do
  def error(error, s) do
    reply(error, s)
    {:stop, error, s}
  end

  def reply(_reply, %{}) do
  end

  def reply(reply, {_, _} = from) do
    GenServer.reply(from, reply)
    true
  end
end
