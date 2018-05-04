defmodule Mongo.PBKDF2 do
  # From https://github.com/elixir-lang/plug/blob/ef616a9db9c87ec392dd8a0949bc52fafcf37005/lib/plug/crypto/key_generator.ex
  # with modifications

  @moduledoc """
  `PBKDF2` implements PBKDF2 (Password-Based Key Derivation Function 2),
  part of PKCS #5 v2.0 (Password-Based Cryptography Specification).
  It can be used to derive a number of keys for various purposes from a given
  secret. This lets applications have a single secure secret, but avoid reusing
  that key in multiple incompatible contexts.
  see http://tools.ietf.org/html/rfc2898#section-5.2
  """

  use Bitwise
  @max_length bsl(1, 32) - 1

  @doc """
  Returns a derived key suitable for use.
  ## Options
    * `:iterations` - defaults to 1000 (increase to at least 2^16 if used for
      passwords)
    * `:length` - a length in octets for the derived key. Defaults to 32
    * `:digest` - an hmac function to use as the pseudo-random function.
      Defaults to `:sha256`
  """
  def generate(secret, salt, opts \\ []) do
    iterations = Keyword.get(opts, :iterations, 1000)
    length = Keyword.get(opts, :length, 32)
    digest = Keyword.get(opts, :digest, :sha256)

    if length > @max_length do
      raise ArgumentError, "length must be less than or equal to #{@max_length}"
    else
      generate(mac_fun(digest, secret), salt, iterations, length, 1, [], 0)
    end
  end

  defp generate(_fun, _salt, _iterations, max_length, _block_index, acc, length)
      when length >= max_length do
    key = acc |> Enum.reverse |> IO.iodata_to_binary
    <<bin::binary-size(max_length), _::binary>> = key
    bin
  end

  defp generate(fun, salt, iterations, max_length, block_index, acc, length) do
    initial = fun.(<<salt::binary, block_index::integer-size(32)>>)
    block   = iterate(fun, iterations - 1, initial, initial)
    generate(fun, salt, iterations, max_length, block_index + 1,
             [block | acc], byte_size(block) + length)
  end

  defp iterate(_fun, 0, _prev, acc), do: acc

  defp iterate(fun, iteration, prev, acc) do
    next = fun.(prev)
    iterate(fun, iteration - 1, next, :crypto.exor(next, acc))
  end

  defp mac_fun(digest, secret) do
    &:crypto.hmac(digest, secret, &1)
  end
end
