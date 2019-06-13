defmodule Mongo.Auth.X509 do
  @moduledoc false
  import Mongo.Protocol.Utils

  def auth({username, _password}, s) do
    cmd = [authenticate: 1, user: username, mechanism: "MONGODB-X509"]
    with {:ok, _message} <- command(-2, cmd, s) do
      :ok
    else
      _error ->
        {:error, "X509 auth failed"}
    end
  end

end
