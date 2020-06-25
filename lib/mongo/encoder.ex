defprotocol Mongo.Encoder do
  @fallback_to_any false

  def encode(value)
end

defimpl Mongo.Encoder, for: Map do
  def encode(v), do: v
end
