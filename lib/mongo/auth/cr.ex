defmodule Mongo.Auth.CR do
  @moduledoc false
  import Mongo.Protocol.Utils

  def auth({username, password}, s) do
    with {:ok, message} <- command(-2, [getnonce: 1], s),
         do: nonce(message, username, password, s)
  end

  defp nonce(%{"nonce" => nonce, "ok" => 1.0}, username, password, s) do
    digest = digest(nonce, username, password)
    command = [authenticate: 1, user: username, nonce: nonce, key: digest]

    case command(-3, command, s) do
      {:ok, %{"ok" => 1.0}} ->
        :ok
      {:ok, %{"ok" => 0.0, "errmsg" => reason, "code" => code}} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}': #{reason}", code: code)}
      {:ok, nil} ->
        {:error, Mongo.Error.exception(message: "auth failed for '#{username}'")}
      {:error, _} = error ->
        error
    end
  end
end
