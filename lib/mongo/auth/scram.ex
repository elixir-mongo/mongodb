defmodule Mongo.Auth.SCRAM do
  @moduledoc false
  import Mongo.BinaryUtils
  import Mongo.Protocol.Utils
  import Bitwise

  def auth({username, password}, s) do
    # TODO: Wrap and log error

    nonce      = nonce()
    first_bare = first_bare(username, nonce)
    payload    = first_message(first_bare)
    message    = [saslStart: 1, mechanism: "SCRAM-SHA-1", payload: payload]

    result =
      with {:ok, %{"ok" => ok} = reply} when ok == 1 <- command(-2, message, s),
           {message, signature} = first(reply, first_bare, username, password, nonce),
           {:ok, %{"ok" => ok} = reply} when ok == 1 <- command(-3, message, s),
           message = second(reply, signature),
           {:ok, %{"ok" => ok} = reply} when ok == 1 <- command(-4, message, s),
           do: final(reply)

    case result do
      :ok ->
        :ok
      {:ok, %{"ok" => z, "errmsg" => reason, "code" => code}} when z == 0 ->
        {:error, Mongo.Error.exception(message: "auth failed for user #{username}: #{reason}", code: code)}
      error ->
        error
    end
  end

  defp first(%{"conversationId" => 1, "payload" => server_payload, "done" => false},
             first_bare, username, password, client_nonce) do
    params          = parse_payload(server_payload)
    server_nonce    = params["r"]
    salt            = params["s"] |> Base.decode64!
    iter            = params["i"] |> String.to_integer
    pass            = digest_password(username, password)
    salted_password = hi(pass, salt, iter)

    <<^client_nonce::binary(24), _::binary>> = server_nonce

    client_message       = "c=biws,r=#{server_nonce}"
    auth_message         = "#{first_bare},#{server_payload.binary},#{client_message}"
    server_signature     = generate_signature(salted_password, auth_message)
    proof                = generate_proof(salted_password, auth_message)
    client_final_message = %BSON.Binary{binary: "#{client_message},#{proof}"}
    message              = [saslContinue: 1, conversationId: 1, payload: client_final_message]

    {message, server_signature}
  end

  defp second(%{"conversationId" => 1, "payload" => payload, "done" => false}, signature) do
    params = parse_payload(payload)
    ^signature = params["v"] |> Base.decode64!
    [saslContinue: 1, conversationId: 1, payload: %BSON.Binary{binary: ""}]
  end

  defp final(%{"conversationId" => 1, "payload" => %BSON.Binary{binary: ""}, "done" => true}) do
    :ok
  end

  defp first_message(first_bare) do
    %BSON.Binary{binary: "n,,#{first_bare}"}
  end

  defp first_bare(username, nonce) do
    "n=#{encode_username(username)},r=#{nonce}"
  end

  defp hi(password, salt, iterations) do
    Mongo.PBKDF2Cache.pbkdf2(password, salt, iterations)
  end

  defp generate_proof(salted_password, auth_message) do
    client_key   = :crypto.hmac(:sha, salted_password, "Client Key")
    stored_key   = :crypto.hash(:sha, client_key)
    signature    = :crypto.hmac(:sha, stored_key, auth_message)
    client_proof = xor_keys(client_key, signature, "")
    "p=#{Base.encode64(client_proof)}"
  end

  defp generate_signature(salted_password, auth_message) do
    server_key = :crypto.hmac(:sha, salted_password, "Server Key")
    :crypto.hmac(:sha, server_key, auth_message)
  end

  defp xor_keys("", "", result),
    do: result
  defp xor_keys(<<fa, ra::binary>>, <<fb, rb::binary>>, result),
    do: xor_keys(ra, rb, <<result::binary, fa ^^^ fb>>)


  defp nonce do
    :crypto.strong_rand_bytes(18)
    |> Base.encode64
  end

  defp encode_username(username) do
    username
    |> String.replace("=", "=3D")
    |> String.replace(",", "=2C")
  end

  defp parse_payload(%BSON.Binary{subtype: :generic, binary: payload}) do
    payload
    |> String.split(",")
    |> Enum.into(%{}, &List.to_tuple(String.split(&1, "=", parts: 2)))
  end
end
