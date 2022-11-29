# Used by "mix format"
locals_without_parens = [
  # MongoDB
  after_load: :*,
  before_dump: :*,
  attribute: :*,
  collection: :*,
  embeds_one: :*,
  embeds_many: :*,
  # Test
  ## Assertions
  assert_receive_event: :*,
  refute_receive_event: :*
]

[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  line_length: 250,
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
