defmodule BSONTest do
  use ExUnit.Case

  test "encode 1" do
    assert encode(%{hello: "world"}) == <<22,0,0,0,2,104,101,108,108,111,0,6,0,0,0,119,111,114,108,100,0,0>>
  end

  test "encode 2" do
    assert encode(%{BSON: ["awesome", 5.05, 1986]}) == <<49,0,0,0,4,66,83,79,78,0,38,0,0,0,2,48,0,8,0,0,0,97,119,101,115,111,109,101,0,1,49,0,51,51,51,51,51,51,20,64,16,50,0,194,7,0,0,0,0>>
  end

  test "decode 1" do
    assert BSON.decode(<<22,0,0,0,2,104,101,108,108,111,0,6,0,0,0,119,111,114,108,100,0,0>>) == %{"hello" => "world"}
  end

  test "decode 2" do
    assert BSON.decode(<<49,0,0,0,4,66,83,79,78,0,38,0,0,0,2,48,0,8,0,0,0,97,119,101,115,111,109,101,0,1,49,0,51,51,51,51,51,51,20,64,16,50,0,194,7,0,0,0,0>>) == %{"BSON" => ["awesome", 5.05, 1986]}
  end

  defp encode(value) do
    value
    |> BSON.encode
    |> IO.iodata_to_binary
  end
end
