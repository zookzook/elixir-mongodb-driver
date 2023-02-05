defmodule Mongo.UpdateHintTest do
  use CollectionCase

  test "update_one, using :hint options", %{pid: top} do
    coll = unique_collection()

    Mongo.insert_one(top, coll, %{_id: 1, member: "abc123", status: "P", points: 0, misc1: nil, misc2: nil})
    Mongo.insert_one(top, coll, %{_id: 2, member: "xyz123", status: "A", points: 60, misc1: "reminder: ping me at 100pts", misc2: "Some random comment"})
    Mongo.insert_one(top, coll, %{_id: 3, member: "lmn123", status: "P", points: 0, misc1: nil, misc2: nil})
    Mongo.insert_one(top, coll, %{_id: 4, member: "pqr123", status: "D", points: 20, misc1: "Deactivated", misc2: nil})
    Mongo.insert_one(top, coll, %{_id: 5, member: "ijk123", status: "P", points: 0, misc1: nil, misc2: nil})
    Mongo.insert_one(top, coll, %{_id: 6, member: "cde123", status: "A", points: 86, misc1: "reminder: ping me at 100pts", misc2: "Some random comment"})

    assert :ok = Mongo.create_indexes(top, coll, [%{key: %{status: 1}, name: "status_index"}, %{key: %{points: 1}, name: "points_index"}])

    assert {:ok,
            %Mongo.UpdateResult{
              acknowledged: true,
              matched_count: 3,
              modified_count: 3,
              upserted_ids: []
            }} = Mongo.update_many(top, coll, %{points: %{"$lte": 20}, status: "P"}, %{"$set": %{misc1: "Need to activate"}}, hint: %{status: 1})

    assert {:error, %{write_errors: [%{"code" => 2, "index" => 0}]}} = Mongo.update_many(top, coll, %{points: %{"$lte": 20}, status: "P"}, %{"$set": %{misc1: "Need to activate"}}, hint: %{email: 1})
  end
end
