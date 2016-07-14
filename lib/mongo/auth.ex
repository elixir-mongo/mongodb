defmodule Mongo.Auth do
  @moduledoc false

  def run(opts, s) do
    auth = setup(opts)
    auther = mechanism(s)

    auth_source = opts[:auth_source]

    if auth_source != nil do
      s = Map.put(s, :database, auth_source)
    end
    Enum.find_value(auth, fn opts ->
      case auther.auth(opts, s) do
        :ok ->
          nil
        error ->
          error
      end
    end) || {:ok, Map.put(s,:database, opts[:database])}
  end

  defp setup(opts) do
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
