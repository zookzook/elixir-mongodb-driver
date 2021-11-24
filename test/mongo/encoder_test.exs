defmodule Mongo.EncoderTest do
  use MongoTest.Case, async: false
  alias Mongo

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    Mongo.drop_database(pid)
    {:ok, [pid: pid]}
  end

  defimpl Mongo.Encoder, for: Date do
    def encode(date) do
      Date.to_iso8601(date)
    end
  end

  defmodule CustomStructWithoutProtocol do
    @fields [:a, :b, :c, :id]
    @enforce_keys @fields
    defstruct @fields
  end

  defmodule CustomStruct do
    @fields [:a, :b, :c, :id]
    @enforce_keys @fields
    defstruct @fields

    defimpl Mongo.Encoder do
      def encode(%{a: a, b: b, id: id}) do
        %{
          _id: id,
          a: a,
          b: b,
          custom_encoded: true
        }
      end
    end
  end

  test "insert encoded date with protocol", c do
    coll = unique_collection()

    to_insert = %{date: ~D[2000-01-01]}

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, to_insert, [])

    assert %{"date" => "2000-01-01"} = Mongo.find_one(c.pid, coll, %{})
  end

  test "insert encoded struct with protocol", c do
    coll = unique_collection()

    struct_to_insert = %CustomStruct{a: 10, b: 20, c: 30, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, struct_to_insert, [])

    assert [
      %{
                  "a" => 10,
                  "b" => 20,
                  "custom_encoded" => true,
                  "_id" => "5ef27e73d2a57d358f812001"
                }
              ] = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()
  end

  test "insert encoded struct without protocol", c do
    coll = unique_collection()

    struct_to_insert = %CustomStructWithoutProtocol{a: 10, b: 20, c: 30, id: "x"}

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, struct_to_insert, [])
    assert [%{"a" => 10, "b" => 20, "c" => 30, "id" => "x"}] = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()
  end

  defimpl Mongo.Encoder, for: Function do
    def encode(_), do: %{fun: true, _id: "5ef27e73d2a57d358f812002"}
  end

  test "insert encoded function to db", c do
    coll = unique_collection()

    fun_to_insert = & &1

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, fun_to_insert, [])

    assert [%{"fun" => true, "_id" => "5ef27e73d2a57d358f812002"}]
        = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()
  end

  test "update with encoded struct in db with protocol", c do
    coll = unique_collection()

    struct_to_insert = %CustomStruct{a: 10, b: 20, c: 30, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, struct_to_insert, [])

    struct_to_change = %CustomStruct{a: 100, b: 200, c: 300, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} =
             Mongo.update_one(c.pid, coll, %{_id: "5ef27e73d2a57d358f812001"}, %{
               "$set": struct_to_change
             })

    assert [
                %{
                  "a" => 100,
                  "b" => 200,
                  "custom_encoded" => true,
                  "_id" => "5ef27e73d2a57d358f812001"
                }
              ] = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()
  end

  test "upsert with encoded struct in db with protocol", c do
    coll = unique_collection()

    struct_to_change = %CustomStruct{a: 100, b: 200, c: 300, id: "5ef27e73d2a57d358f812001"}

    assert {:ok, _} =
             Mongo.update_one(
               c.pid,
               coll,
               %{_id: "5ef27e73d2a57d358f812001"},
               %{"$set": struct_to_change},
               upsert: true
             )

    assert  [
                %{
                  "a" => 100,
                  "b" => 200,
                  "custom_encoded" => true,
                  "_id" => "5ef27e73d2a57d358f812001"
                }
              ] = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()
  end
end
