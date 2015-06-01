defmodule Mongo.Connection.Auth do
  @moduledoc false
  import Mongo.Protocol
  import Mongo.Connection.Utils

  def setup(%{auth: nil, opts: opts} = s) do
    database = opts[:database]
    username = opts[:username]
    password = opts[:password]
    auth     = opts[:auth] || []

    auth =
      Enum.map(auth, fn opts ->
        database = opts[:database]
        username = opts[:username]
        password = opts[:password]
        {database, username, password}
      end)

    if database && username && password do
      auth = auth ++ [{database, username, password}]
    end

    if auth != [] do
      database = s.database || (auth |> List.last |> elem(0))
    end

    opts = Keyword.drop(opts, ~w(database username password auth)a)
    %{s | auth: auth, opts: opts, database: database}
  end

  def init(%{auth: auth} = s) do
    Enum.find_value(auth, fn opts ->
      case inactive_auth(opts, s) do
        :ok ->
          nil
        {:error, _} = error ->
          error
      end
    end) || :ok
  end

  defp inactive_auth({database, username, password}, s) do
    case inactive_command(-1, database, %{getnonce: 1}, s) do
      {:ok, %{"nonce" => nonce, "ok" => 1.0}} ->
        inactive_digest(nonce, database, username, password, s)
      {:tcp_error, _} = error ->
        error
    end
  end

  defp inactive_digest(nonce, database, username, password, s) do
    digest = digest(nonce, username, password)
    command = %{authenticate: 1, user: username, nonce: nonce, key: digest}

    case inactive_command(-2, database, command, s) do
      {:ok, %{"ok" => 1.0}} ->
        :ok
      {:ok, %{"ok" => 0.0, "errmsg" => reason, "code" => code}} ->
        {:error, %Mongo.Error{message: "auth failed for '#{username}': #{reason}", code: code}}
      {:ok, nil} ->
        {:error, %Mongo.Error{message: "auth failed for '#{username}'"}}
      {:tcp_error, _} = error ->
        error
    end
  end

  defp inactive_command(id, database, command, s) do
    op = op_query(coll: namespace({database, "$cmd"}, s), query: command,
                  select: nil, num_skip: 0, num_return: 1, flags: [])
    case send(op, id, s) do
      {:ok, s} ->
        case inactive_recv(s) do
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

  defp inactive_recv(tail \\ "", s) do
    case :gen_tcp.recv(s.socket, 0, s.timeout) do
      {:ok, data} ->
        data = tail <> data
        case decode(data) do
          {:ok, id, reply, ""} ->
            {:ok, id, reply}
          :error ->
            inactive_recv(data, s)
        end

      {:error, reason} ->
        {:tcp_error, reason}
    end
  end
end
