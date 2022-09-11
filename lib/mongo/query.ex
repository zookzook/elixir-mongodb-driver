defmodule Mongo.Query do
  @moduledoc """
    This is the query implementation for the Query Protocol

  Encoding and decoding does not take place at this point, but is directly performed
    into the functions of Mongo.MongoDBConnection.Utils.
  """
  defstruct action: nil
end

defimpl DBConnection.Query, for: Mongo.Query do
  # coveralls-ignore-start
  # gets never called
  def parse(query, _opts), do: query
  # gets never called
  def describe(query, _opts), do: query
  # coveralls-ignore-stop
  def encode(_query, params, _opts), do: params
  def decode(_query, reply, _opts), do: reply
end
