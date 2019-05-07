defmodule Mongo.UnorderedBulk do
  @moduledoc """

  todo

  Ist immer für eine Collections

  """

  alias Mongo.UnorderedBulk

  defstruct inserts: [], updates: [], deletes: [], opts: []

  def new() do
    %UnorderedBulk{}
  end

  def insert_one(%UnorderedBulk{inserts: rest} = b, doc) do
    %UnorderedBulk{b | inserts: [doc | rest] }
  end

  def delete_one(%UnorderedBulk{deletes: rest} = b, doc) do
    %UnorderedBulk{b | deletes: [doc | rest] }
  end

  def update_one(%UnorderedBulk{updates: rest} = b, filter, update) do
    %UnorderedBulk{b | updates: [{filter, update} | rest] }
  end

end