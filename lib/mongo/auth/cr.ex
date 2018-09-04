defmodule Mongo.Auth.CR do
  @moduledoc false
  import Mongo.Protocol.Utils

  def auth({username, password}, s) do
    with {:ok, message} <- command(-2, [getnonce: 1], s),
         do: nonce(message, username, password, s)
  end

  defp nonce(%{"nonce" => nonce, "ok" => ok}, username, password, s)
  when ok == 1 # to support a response that returns 1 or 1.0
  do
    digest = digest(nonce, username, password)
    command = [authenticate: 1, user: username, nonce: nonce, key: digest]

    case command(-3, command, s) do
      {:ok, %{"ok" => ok}} when ok == 1 ->
        :ok
      {:ok, %{"ok" => 0.0, "errmsg" => reason, "code" => code}} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}': #{reason}", code: code)}
      {:ok, nil} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}'")}
      error ->
        error
    end
  end
end
