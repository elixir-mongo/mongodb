defmodule Mongo.Auth do
  @moduledoc false

  def run(opts, s) do
    auth = setup(opts)
    auther = mechanism(s)

    auth_source = opts[:auth_source]
    wire_version = s[:wire_version]

    s = if auth_source != nil && wire_version > 0,
          do: Map.put(s, :database, auth_source),
        else: s

    Enum.find_value(auth, fn opts ->
      case auther.auth(opts, s) do
        :ok ->
          nil
        error ->
          {mod, sock} = s.socket
          mod.close(sock)
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

  defp mechanism(%{wire_version: version, auth_mechanism: :x509}) when version >= 3,
    do: Mongo.Auth.X509
  defp mechanism(%{wire_version: version}) when version >= 3,
    do: Mongo.Auth.SCRAM
  defp mechanism(_),
    do: Mongo.Auth.CR
end
