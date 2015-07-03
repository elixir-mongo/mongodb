defmodule Mongo.Connection.Auth.SCRAM do
  @moduledoc false
  import Mongo.BinaryUtils
  import Mongo.Connection.Utils
  import Bitwise

  @random_length 24

  def auth({username, password}, s) do
    # TODO: Wrap and log error
    nonce      = nonce()
    first_bare = first_bare(username, nonce)
    payload    = first_message(first_bare)
    message    = [saslStart: 1, mechanism: "SCRAM-SHA-1", payload: payload]

    case sync_command(-2, message, s) do
      {:ok, %{"conversationId" => 1, "payload" => server_payload, "done" => false, "ok" => 1.0}} ->
        conversation_first(server_payload, first_bare, username, password, nonce, s)
      error ->
        handle_error(error, username)
    end
  end

  defp conversation_first(server_payload, first_bare, username, password, nonce, s) do
    {signature, payload} = second_message(server_payload, first_bare, username, password, nonce)
    message = [saslContinue: 1, conversationId: 1, payload: payload]

    case sync_command(-3, message, s) do
      {:ok, %{"conversationId" => 1, "payload" => payload, "done" => false, "ok" => 1.0}} ->
        conversation_second(payload, signature, username, s)
      error ->
        handle_error(error, username)
    end
  end

  defp conversation_second(payload, signature, username, s) do
    params = parse_payload(payload)
    ^signature = params["v"] |> Base.decode64!

    payload = %BSON.Binary{binary: ""}
    message = [saslContinue: 1, conversationId: 1, payload: payload]

    case sync_command(-4, message, s) do
      {:ok, %{"conversationId" => 1, "payload" => payload, "done" => true, "ok" => 1.0}} ->
        %BSON.Binary{binary: ""} = payload
        :ok
      error ->
        handle_error(error, username)
    end
  end

  defp first_message(first_bare) do
    %BSON.Binary{binary: "n,,#{first_bare}"}
  end

  defp first_bare(username, nonce) do
    "n=#{encode_username(username)},r=#{nonce}"
  end

  defp second_message(payload, first_bare, username, password, client_nonce) do
    params          = parse_payload(payload)
    server_nonce    = params["r"]
    salt            = params["s"] |> Base.decode64!
    iter            = params["i"] |> String.to_integer
    pass            = digest_password(username, password)
    salted_password = hi(pass, salt, iter)

    <<^client_nonce::binary(24), _::binary>> = server_nonce

    client_message       = "c=biws,r=#{server_nonce}"
    auth_message         = "#{first_bare},#{payload.binary},#{client_message}"
    server_signature     = generate_signature(salted_password, auth_message)
    proof                = generate_proof(salted_password, auth_message)
    client_final_message = %BSON.Binary{binary: "#{client_message},#{proof}"}
    {server_signature, client_final_message}
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

  defp handle_error({:tcp_error, _} = error, _username),
    do: error
  defp handle_error({:ok, %{"ok" => 0.0, "errmsg" => reason, "code" => code}}, username),
    do: {:error, %Mongo.Error{message: "auth failed for '#{username}': #{reason}", code: code}}
end
