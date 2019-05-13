defmodule Mongo.Utils do


  def filter_nils(keyword) when is_list(keyword) do
    Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
  end

  def filter_nils(map) when is_map(map) do
    Enum.reject(map, fn {_key, value} -> is_nil(value) end)
    |> Enum.into(%{})
  end


  def assign_ids(list) when is_list(list) do
    Enum.map(list, &assign_id/1)
    |> Enum.unzip
  end

  defp assign_id(%{_id: id} = map) when id != nil,  do: {id, map}
  defp assign_id(%{"_id" => id} = map) when id != nil, do: {id, map}
  defp assign_id([{_, _} | _] = keyword) do
    case Keyword.take(keyword, [:_id, "_id"]) do
      [{_key, id} | _] when id != nil -> {id, keyword}
      [] -> add_id(keyword)
    end
  end

  defp assign_id(map) when is_map(map) do
    map |> Map.to_list |> add_id
  end

  ##
  # Inserts an ID to the document. A distinction is made as to whether binaries or atoms are used as keys.
  #
  defp add_id(doc) do
    id = Mongo.IdServer.new
    {id, add_id(doc, id)}
  end
  defp add_id([{key, _}|_] = list, id) when is_atom(key), do: [{:_id, id}|list]
  defp add_id([{key, _}|_] = list, id) when is_binary(key), do: [{"_id", id}|list]
  defp add_id([], id), do: [{"_id", id}]

end
