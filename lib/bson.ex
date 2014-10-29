defmodule BSON do
  def encode(map) when is_map(map) do
    BSON.Encoder.encode(map)
  end

  def decode(binary) when is_binary(binary) do
    BSON.Decoder.decode(binary)
  end
end
