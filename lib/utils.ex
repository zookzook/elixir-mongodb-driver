defmodule Mongo.Utils do
  @moduledoc false

  def assign_ids(list) when is_list(list) do
    list
    |> Enum.map(fn item ->
      case Mongo.Encoder.impl_for(item) do
        nil ->
          item

        _ ->
          Mongo.Encoder.encode(item)
      end
    end)
    |> Enum.map(fn item -> assign_id(item) end)
    |> Enum.unzip()
  end

  defp assign_id(%{_id: id} = map) when id != nil, do: {id, map}
  defp assign_id(%{"_id" => id} = map) when id != nil, do: {id, map}

  defp assign_id([{_, _} | _] = keyword) do
    case Keyword.take(keyword, [:_id, "_id"]) do
      [{_key, id} | _] when id != nil -> {id, keyword}
      [] -> add_id(keyword)
    end
  end

  defp assign_id(map) when is_map(map) do
    map
    |> Map.to_list()
    |> add_id()
  end

  ##
  # Inserts an ID to the document. A distinction is made as to whether binaries or atoms are used as keys.
  #
  defp add_id(doc) do
    id = Mongo.IdServer.new()
    {id, add_id(doc, id)}
  end

  defp add_id([{key, _} | _] = list, id) when is_atom(key), do: [{:_id, id} | list]
  defp add_id([{key, _} | _] = list, id) when is_binary(key), do: [{"_id", id} | list]
  defp add_id([], id), do: [{"_id", id}]

  def modifier_docs([{key, _} | _], type), do: key |> key_to_string |> modifier_key(type)
  def modifier_docs(map, _type) when is_map(map) and map_size(map) == 0, do: :ok
  def modifier_docs(map, type) when is_map(map), do: Enum.at(map, 0) |> elem(0) |> key_to_string |> modifier_key(type)
  def modifier_docs(list, type) when is_list(list), do: Enum.map(list, &modifier_docs(&1, type))

  defp modifier_key(<<?$, _::binary>> = other, :replace), do: raise(ArgumentError, "replace does not allow atomic modifiers, got: #{other}")
  defp modifier_key(<<?$, _::binary>>, :update), do: :ok
  defp modifier_key(<<_, _::binary>> = other, :update), do: raise(ArgumentError, "update only allows atomic modifiers, got: #{other}")
  defp modifier_key(_, _), do: :ok

  defp key_to_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_to_string(key) when is_binary(key), do: key
end
