defmodule BSONTest do
  use ExUnit.Case

  test "encode" do
    assert encode(%{hello: "world"}) == "\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00"
  end

  defp encode(value) do
    value
    |> BSON.encode
    |> IO.iodata_to_binary
  end
end
