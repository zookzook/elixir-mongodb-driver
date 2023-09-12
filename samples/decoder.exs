items = Enum.map(1..100, fn i -> %{id: :crypto.strong_rand_bytes(10), age: i, name: "Greta"} end)

doc = %{
  name: "This is a test",
  items: items
}

encoded = BSON.encode(doc)

IO.inspect(BSON.decode(encoded))

Benchee.run(
  %{
    "Original encoder" => fn -> BSON.decode(encoded) end,
    "Original encoder 1" => fn -> BSON.decode(encoded) end,
    "Original encoder 2" => fn -> BSON.decode(encoded) end,
    "Original encoder 3" => fn -> BSON.decode(encoded) end,
  }
)
