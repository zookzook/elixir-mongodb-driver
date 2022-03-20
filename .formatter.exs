# Used by "mix format"
[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  line_length: 250,
  locals_without_parens: [
    # MongoDB
    after_load: :*,
    before_dump: :*,
    attribute: :*,
    collection: :*,
    embeds_many: :*,
    # Test
    ## Assertions
    assert_receive_event: :*,
    refute_receive_event: :*
  ]
]
