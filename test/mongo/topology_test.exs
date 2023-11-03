defmodule Mongo.TopologyTest do
  # DO NOT MAKE ASYNCHRONOUS
  use ExUnit.Case

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect()
    %{pid: pid}
  end

  @modes [:secondary, :secondary_preferred, :primary, :primary_preferred]

  @tag :rs_required
  test "replica set selection", %{pid: mongo_pid} do
    for mode <- @modes do
      assert {:ok, %Mongo.InsertOneResult{inserted_id: new_id}} = Mongo.insert_one(mongo_pid, "test", %{topology_test: 1}, w: 3)

      rp = Mongo.ReadPreference.merge_defaults(%{mode: mode})

      assert [%{"_id" => ^new_id, "topology_test" => 1}] =
               mongo_pid
               |> Mongo.find("test", %{_id: new_id}, read_preference: rp, slave_ok: mode in [:secondary, :secondary_preferred])
               |> Enum.to_list()

      assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} = Mongo.delete_one(mongo_pid, "test", %{_id: new_id})
    end
  end
end
