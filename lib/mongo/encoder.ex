defprotocol Mongo.Encoder do
  @fallback_to_any false

  @spec encode(t) :: map()
  def encode(value)
end

defimpl Mongo.Encoder, for: Map do
  def encode(v), do: v
end

defimpl Mongo.Encoder, for: List do
  def encode(v), do: v
end
