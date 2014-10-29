defmodule BSON do
  def encode(map) when is_map(map) do
    BSON.Encoder.encode(map)
  end
end
