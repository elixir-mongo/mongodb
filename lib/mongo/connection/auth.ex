defmodule Mongo.Connection.Auth do
  @moduledoc false

  def setup(%{auth: nil, opts: opts} = s) do
    database = opts[:database]
    username = opts[:username]
    password = opts[:password]
    auth     = opts[:auth] || []

    auth =
      Enum.map(auth, fn opts ->
        username = opts[:username]
        password = opts[:password]
        {username, password}
      end)

    if username && password do
      auth = auth ++ [{username, password}]
    end

    opts = Keyword.drop(opts, ~w(database username password auth)a)
    %{s | auth: auth, opts: opts, database: database}
  end

  def run(%{auth: auth} = s) do
    auther = mechanism(s)

    Enum.find_value(auth, fn opts ->
      case auther.auth(opts, s) do
        :ok ->
          nil
        error ->
          error
      end
    end) || {:ok, s}
  end

  defp mechanism(%{wire_version: version}) when version >= 3,
    do: Mongo.Connection.Auth.SCRAM
  defp mechanism(_),
    do: Mongo.Connection.Auth.CR
end
