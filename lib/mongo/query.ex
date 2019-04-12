defmodule Mongo.Query do
  @moduledoc """
    This is the query implementation for the Query Protocoll

    The action attribute describes as atom the desired action. There are currently two
    * :command
    * :wire_version
      
    Encoding and decoding does not take place at this point, but is directly performed
    into the functions of Mongo.MongoDBConnection.Utils.
  """
  defstruct action: nil
end

defimpl DBConnection.Query, for: Mongo.Query do
  def parse(query, _opts), do: query
  def describe(query, _opts), do: query
  def encode(query, params, _opts), do: params
  def decode(_query, :ok, _opts), do: :ok
  def decode(_query, wire_version, _opts) when is_integer(wire_version), do: wire_version
  def decode(_query, reply, _opts), do: reply
end
