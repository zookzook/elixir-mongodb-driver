defprotocol Mongo.Encoder do
  @fallback_to_any false

  @spec encode(t) :: map()
  def encode(value)
end
