defmodule Mongo.Auth do
  @moduledoc false

  def run(opts, s) do
    auth = setup(opts)
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

  defp setup(opts) do
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

    if username && password, do: auth ++ [{username, password}], else: auth
  end

  defp mechanism(%{wire_version: version}) when version >= 3,
    do: Mongo.Auth.SCRAM
  defp mechanism(_),
    do: Mongo.Auth.CR
end
