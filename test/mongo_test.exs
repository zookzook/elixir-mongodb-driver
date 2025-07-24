defmodule Mongo.Test do
  use ExUnit.Case

  import CollectionCase, only: [unique_collection: 0]

  defmodule TestUser do
    defstruct name: "John", age: 27

    defimpl Mongo.Encoder do
      def encode(m), do: Map.drop(m, [:__struct__])
    end
  end

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect()
    Mongo.drop_database(pid, nil, w: 3)
    {:ok, [pid: pid]}
  end

  defmacro unique_name do
    {function, _arity} = __CALLER__.function

    "#{__CALLER__.module}.#{function}"
    |> String.replace(" ", "_")
    |> String.replace(".", "_")
    |> String.downcase()
  end

  test "object_id" do
    assert %BSON.ObjectId{value: <<_::96>>} = Mongo.object_id()
  end

  test "command", c do
    assert {:ok, %{"ok" => 1.0}} = Mongo.command(c.pid, ping: true)
    assert {:error, %Mongo.Error{}} = Mongo.command(c.pid, xdrop: "unexisting-database")
  end

  test "command!", c do
    assert %{"ok" => 1.0} = Mongo.command!(c.pid, ping: true)
    assert_raise Mongo.Error, fn -> Mongo.command!(c.pid, xdrop: "unexisting-database") end
  end

  test "show_collections", c do
    coll_1 = unique_name() <> "_1"
    coll_2 = unique_name() <> "_2"

    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 1})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 2})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_2, %{foo: 3})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_2, %{foo: 4})

    cmd = [createIndexes: coll_1, indexes: [[key: [foo: 1], name: "not-a-collection"]]]
    assert {:ok, _} = Mongo.command(c.pid, cmd)

    cmd = [createIndexes: coll_2, indexes: [[key: [foo: 1, bar: 1], name: "not-a-collection"]]]
    assert {:ok, _} = Mongo.command(c.pid, cmd)

    colls =
      c.pid
      |> Mongo.show_collections()
      |> Enum.to_list()

    assert Enum.member?(colls, coll_1)
    assert Enum.member?(colls, coll_2)
    assert not Enum.member?(colls, "not-a-collection")
  end

  test "list_indexes", c do
    coll_1 = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 1})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 2})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 3})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll_1, %{foo: 4})

    cmd = [createIndexes: coll_1, indexes: [[key: [foo: 1], name: "foo"]]]
    assert {:ok, _} = Mongo.command(c.pid, cmd)

    cmd = [createIndexes: coll_1, indexes: [[key: [foo: 1, bar: 1], name: "foo-bar"]]]
    assert {:ok, _} = Mongo.command(c.pid, cmd)

    indexes =
      c.pid
      |> Mongo.list_index_names(coll_1)
      |> Enum.to_list()

    assert Enum.count(indexes) == 3
    assert Enum.member?(indexes, "_id_")
    assert Enum.member?(indexes, "foo")
    assert Enum.member?(indexes, "foo-bar")
  end

  test "aggregate", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 44})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 45})

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}, %{"foo" => 45}] = Mongo.aggregate(c.pid, coll, []) |> Enum.to_list()

    query = [
      %{
        "$match" => %{
          "foo" => %{"$gt" => 43}
        }
      },
      %{
        "$group" => %{
          "_id" => "foo",
          "total" => %{"$sum" => "$foo"}
        }
      }
    ]

    assert [%{"_id" => "foo", "total" => 89}] = Mongo.aggregate(c.pid, coll, query) |> Enum.to_list()

    assert [] = Mongo.aggregate(c.pid, coll, []) |> Enum.take(0)
    assert [] = Mongo.aggregate(c.pid, coll, []) |> Enum.drop(4)
    assert [%{"foo" => 42}] = Mongo.aggregate(c.pid, coll, []) |> Enum.take(1)
    assert [%{"foo" => 45}] = Mongo.aggregate(c.pid, coll, []) |> Enum.drop(3)

    assert [] = Mongo.aggregate(c.pid, coll, [], use_cursor: false) |> Enum.take(0)
    assert [] = Mongo.aggregate(c.pid, coll, [], use_cursor: false) |> Enum.drop(4)
    assert [%{"foo" => 42}] = Mongo.aggregate(c.pid, coll, [], use_cursor: false) |> Enum.take(1)
    assert [%{"foo" => 45}] = Mongo.aggregate(c.pid, coll, [], use_cursor: false) |> Enum.drop(3)

    assert [] = Mongo.aggregate(c.pid, coll, [], batch_size: 1) |> Enum.take(0)
    assert [] = Mongo.aggregate(c.pid, coll, [], batch_size: 1) |> Enum.drop(4)
    assert [%{"foo" => 42}] = Mongo.aggregate(c.pid, coll, [], batch_size: 1) |> Enum.take(1)
    assert [%{"foo" => 45}] = Mongo.aggregate(c.pid, coll, [], batch_size: 1) |> Enum.drop(3)
  end

  test "count", c do
    coll = unique_name()

    assert {:ok, 0} = Mongo.count(c.pid, coll, %{})

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})

    assert {:ok, 2} = Mongo.count(c.pid, coll, %{})
    assert {:ok, 1} = Mongo.count(c.pid, coll, %{foo: 42})
  end

  test "count!", c do
    coll = unique_name()

    assert 0 = Mongo.count!(c.pid, coll, %{foo: 43})
  end

  test "distinct", c do
    coll = unique_name()

    assert {:ok, []} = Mongo.distinct(c.pid, coll, "foo", %{})

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})

    assert {:ok, [42, 43]} = Mongo.distinct(c.pid, coll, "foo", %{})
    assert {:ok, [42]} = Mongo.distinct(c.pid, coll, "foo", %{foo: 42})
  end

  test "distinct!", c do
    coll = unique_name()

    assert [] = Mongo.distinct!(c.pid, coll, "foo", %{})
  end

  test "find", c do
    coll = unique_name()

    assert [] = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43, bar: 2})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 44, bar: 3})

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}] = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()

    assert [%{"foo" => 42}] = Mongo.find(c.pid, coll, %{}, limit: 1) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    assert [%{"foo" => 42}, %{"foo" => 43}, %{"foo" => 44}] = Mongo.find(c.pid, coll, %{}, batch_size: 2) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    assert [%{"foo" => 42}, %{"foo" => 43}] = Mongo.find(c.pid, coll, %{}, limit: 2) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    assert [%{"foo" => 42}, %{"foo" => 43}] = Mongo.find(c.pid, coll, %{}, batch_size: 2, limit: 2) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    assert [%{"foo" => 42}] = Mongo.find(c.pid, coll, %{bar: 1}) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    assert [%{"bar" => 1}, %{"bar" => 2}, %{"bar" => 3}] = Mongo.find(c.pid, coll, %{}, projection: %{bar: 1}) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    assert [%{"bar" => 1}] = Mongo.find(c.pid, coll, %{foo: 42}, projection: %{bar: 1}) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    assert [%{"foo" => 44}, %{"foo" => 43}] = Mongo.find(c.pid, coll, %{}, sort: [foo: -1], batch_size: 2, limit: 2) |> Enum.to_list() |> Enum.map(fn m -> Map.pop(m, "_id") |> elem(1) end)

    # one of error types
    assert {:error, %Mongo.Error{message: "unknown top level operator: $foo. If you have a field name that starts with a '$' symbol, consider using $getField or $setField."}} = Mongo.find(c.pid, coll, %{"$foo" => []})
  end

  test "find_one", c do
    coll = unique_name()

    assert [] = Mongo.find(c.pid, coll, %{}) |> Enum.to_list()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    assert nil == Mongo.find_one(c.pid, coll, %{foo: 43})
    assert %{"foo" => 42} = Mongo.find_one(c.pid, coll, %{})

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43})

    assert %{"foo" => 42} = Mongo.find_one(c.pid, coll, %{})
    # should return the first one so the next test fails
    assert %{"foo" => 43} != Mongo.find_one(c.pid, coll, %{})
  end

  test "find_one_and_update", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    # defaults
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 2}})
    assert %{"bar" => 1} = value, "Should return original document by default"

    # should raise if we don't have atomic operators
    assert_raise ArgumentError, fn ->
      Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{bar: 3})
    end

    # return_document = :after
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 3}}, return_document: :after)
    assert %{"bar" => 3} = value, "Should return modified doc"

    # projection
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 3}}, projection: %{"bar" => 1})
    assert Map.get(value, "foo") == nil, "Should respect the projection"

    # sort
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 10})
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_update(c.pid, coll, %{"foo" => 42}, %{"$set" => %{baz: 1}}, sort: %{"bar" => -1}, return_document: :after)
    assert %{"bar" => 10, "baz" => 1} = value, "Should respect the sort"

    # upsert
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_update(c.pid, coll, %{"foo" => 43}, %{"$set" => %{baz: 1}}, upsert: true, return_document: :after)
    assert %{"foo" => 43, "baz" => 1} = value, "Should upsert"

    # array_filters
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 44, things: [%{id: "123", name: "test"}, %{id: "456", name: "not test"}]})
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_update(c.pid, coll, %{"foo" => 44}, %{"$set" => %{"things.$[sub].name" => "new"}}, array_filters: [%{"sub.id" => "123"}], return_document: :after)
    assert %{"foo" => 44, "things" => [%{"id" => "123", "name" => "new"}, %{"id" => "456", "name" => "not test"}]} = value, "Should leverage array filters"

    # don't find return {:ok, nil}
    assert {:ok, %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, value: nil}} == Mongo.find_one_and_update(c.pid, coll, %{"number" => 666}, %{"$set" => %{title: "the number of the beast"}})

    assert {:ok, %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, value: nil}} == Mongo.find_one_and_update(c.pid, "coll_that_doesnt_exist", %{"number" => 666}, %{"$set" => %{title: "the number of the beast"}})

    # wrong parameter
    assert {:error, %Mongo.Error{}} = Mongo.find_one_and_update(c.pid, 2, %{"number" => 666}, %{"$set" => %{title: "the number of the beast"}})
  end

  test "find_one_and_replace", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    assert_raise ArgumentError, fn ->
      Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 42}, %{"$set" => %{bar: 3}})
    end

    # defaults
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 42}, %{bar: 2})
    assert %{"foo" => 42, "bar" => 1} = value, "Should return original document by default"

    # return_document = :after
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 43, bar: 1})
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 43}, %{bar: 3}, return_document: :after)
    assert %{"bar" => 3} = value, "Should return modified doc"
    assert match?(%{"foo" => 43}, value) == false, "Should replace document"

    # projection
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 44, bar: 1})
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 44}, %{foo: 44, bar: 3}, return_document: :after, projection: %{bar: 1})
    assert Map.get(value, "foo") == nil, "Should respect the projection"

    # sort
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 1, note: "keep"})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 2, note: "replace"})
    assert {:ok, _} = Mongo.find_one_and_replace(c.pid, coll, %{"foo" => 50}, %{foo: 50, bar: 3}, sort: %{bar: -1})
    assert [doc] = Mongo.find(c.pid, coll, %{note: "keep"}) |> Enum.to_list()
    assert %{"bar" => 1, "note" => "keep"} = doc, "Replaced the correct document according to the sort"

    # upsert
    assert [] = Mongo.find(c.pid, coll, %{upsertedDocument: true}) |> Enum.to_list()
    assert {:ok, %Mongo.FindAndModifyResult{value: value}} = Mongo.find_one_and_replace(c.pid, coll, %{"upsertedDocument" => true}, %{"upsertedDocument" => true}, upsert: true, return_document: :after)
    assert %{"upsertedDocument" => true} = value, "Should upsert"
    assert [%{"upsertedDocument" => true}] = Mongo.find(c.pid, coll, %{upsertedDocument: true}) |> Enum.to_list()

    assert {:ok, %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, value: nil}} == Mongo.find_one_and_replace(c.pid, coll, %{"never" => "matching"}, %{})
    assert {:ok, %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, value: nil}} == Mongo.find_one_and_replace(c.pid, "coll_that_doesnt_exist", %{"never" => "matching"}, %{})
  end

  test "find_one_and_delete", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})

    # default
    assert {:ok, %{"foo" => 42, "bar" => 1}} = Mongo.find_one_and_delete(c.pid, coll, %{foo: 42})
    assert [] = Mongo.find(c.pid, coll, %{foo: 42}) |> Enum.to_list()

    # projection
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42, bar: 1})
    assert {:ok, value} = Mongo.find_one_and_delete(c.pid, coll, %{foo: 42}, projection: %{bar: 1})
    assert Map.get(value, "foo") == nil, "Should respect the projection"

    # sort
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 1, note: "keep"})
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 50, bar: 2, note: "delete"})

    assert {:ok, %{"note" => "delete"}} = Mongo.find_one_and_delete(c.pid, coll, %{foo: 50}, sort: %{bar: -1})
    assert [%{"note" => "keep"}] = Mongo.find(c.pid, coll, %{note: "keep"}) |> Enum.to_list()

    assert {:ok, nil} = Mongo.find_one_and_delete(c.pid, coll, %{"never" => "matching"})
  end

  test "insert_one", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.insert_one(c.pid, coll, [%{foo: 42, bar: 1}])
    end

    assert {:ok, result} = Mongo.insert_one(c.pid, coll, %{foo: 42})
    assert %Mongo.InsertOneResult{inserted_id: id} = result

    assert [%{"_id" => ^id, "foo" => 42}] = Mongo.find(c.pid, coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.InsertOneResult{acknowledged: false}} = Mongo.insert_one(c.pid, coll, %{}, w: 0)
  end

  test "insert_one!", c do
    coll = unique_name()

    assert %Mongo.InsertOneResult{} = Mongo.insert_one!(c.pid, coll, %{"_id" => 1})
    assert %Mongo.InsertOneResult{acknowledged: false} == Mongo.insert_one!(c.pid, coll, %{}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.insert_one!(c.pid, coll, %{_id: 1})
    end
  end

  test "insert_many", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.insert_many(c.pid, coll, %{foo: 42, bar: 1})
    end

    assert {:ok, result} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 43}])

    assert %Mongo.InsertManyResult{inserted_ids: [id0, id1]} = result

    assert [%{"_id" => ^id0, "foo" => 42}] = Mongo.find(c.pid, coll, %{_id: id0}) |> Enum.to_list()
    assert [%{"_id" => ^id1, "foo" => 43}] = Mongo.find(c.pid, coll, %{_id: id1}) |> Enum.to_list()

    assert {:ok, %Mongo.InsertManyResult{acknowledged: false}} = Mongo.insert_many(c.pid, coll, [%{}], w: 0)
  end

  test "insert_many!", c do
    coll = unique_name()

    docs = [%{foo: 42}, %{foo: 43}]
    assert %Mongo.InsertManyResult{} = Mongo.insert_many!(c.pid, coll, docs)

    assert %Mongo.InsertManyResult{acknowledged: false} == Mongo.insert_many!(c.pid, coll, [%{}], w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.insert_many!(c.pid, coll, [%{_id: 1}, %{_id: 1}])
    end
  end

  test "delete_one", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} = Mongo.delete_one(c.pid, coll, %{foo: 42})
    assert [%{"foo" => 42}] = Mongo.find(c.pid, coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.DeleteResult{deleted_count: 1}} = Mongo.delete_one(c.pid, coll, %{foo: 42})
    assert [] = Mongo.find(c.pid, coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.DeleteResult{deleted_count: 0}} = Mongo.delete_one(c.pid, coll, %{foo: 42})
    assert [%{"foo" => 43}] = Mongo.find(c.pid, coll, %{foo: 43}) |> Enum.to_list()
  end

  test "delete_one!", c do
    coll = unique_name()

    assert %Mongo.DeleteResult{deleted_count: 0} = Mongo.delete_one!(c.pid, coll, %{foo: 42})

    assert %Mongo.DeleteResult{acknowledged: false, deleted_count: 0} == Mongo.delete_one!(c.pid, coll, %{}, w: 0)
  end

  test "delete_many", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.DeleteResult{deleted_count: 2}} = Mongo.delete_many(c.pid, coll, %{foo: 42})
    assert [] = Mongo.find(c.pid, coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.DeleteResult{deleted_count: 0}} = Mongo.delete_one(c.pid, coll, %{foo: 42})
    assert [%{"foo" => 43}] = Mongo.find(c.pid, coll, %{foo: 43}) |> Enum.to_list()
  end

  test "delete_many!", c do
    coll = unique_name()

    assert %Mongo.DeleteResult{deleted_count: 0} = Mongo.delete_many!(c.pid, coll, %{foo: 42})

    assert %Mongo.DeleteResult{acknowledged: false, deleted_count: 0} == Mongo.delete_many!(c.pid, coll, %{}, w: 0)
  end

  test "replace_one", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.replace_one(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})
    end

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1, upserted_ids: []}} = Mongo.replace_one(c.pid, coll, %{foo: 42}, %{foo: 0})

    assert [_] = Mongo.find(c.pid, coll, %{foo: 0}) |> Enum.to_list()
    assert [_] = Mongo.find(c.pid, coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 0, upserted_ids: [id]}} = Mongo.replace_one(c.pid, coll, %{foo: 50}, %{foo: 0}, upsert: true)
    assert [_] = Mongo.find(c.pid, coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1, upserted_ids: []}} = Mongo.replace_one(c.pid, coll, %{foo: 43}, %{foo: 1}, upsert: true)
    assert [] = Mongo.find(c.pid, coll, %{foo: 43}) |> Enum.to_list()
    assert [_] = Mongo.find(c.pid, coll, %{foo: 1}) |> Enum.to_list()
  end

  test "replace_one!", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{acknowledged: true, matched_count: 0, modified_count: 0, upserted_ids: []} = Mongo.replace_one!(c.pid, coll, %{foo: 43}, %{foo: 0})

    assert %Mongo.UpdateResult{acknowledged: false, matched_count: 0, modified_count: 0, upserted_ids: []} == Mongo.replace_one!(c.pid, coll, %{foo: 45}, %{foo: 0}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.replace_one!(c.pid, coll, %{foo: 42}, %{_id: 1})
    end
  end

  test "update_one", c do
    coll = unique_name()

    assert_raise ArgumentError, fn ->
      Mongo.update_one(c.pid, coll, %{foo: 42}, %{foo: 0})
    end

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1, upserted_ids: []}} = Mongo.update_one(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert [_] = Mongo.find(c.pid, coll, %{foo: 0}) |> Enum.to_list()
    assert [_] = Mongo.find(c.pid, coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 0, upserted_ids: [id]}} = Mongo.update_one(c.pid, coll, %{foo: 50}, %{"$set": %{foo: 0}}, upsert: true)
    assert [_] = Mongo.find(c.pid, coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1, upserted_ids: []}} = Mongo.update_one(c.pid, coll, %{foo: 43}, %{"$set": %{foo: 1}}, upsert: true)
    assert [] = Mongo.find(c.pid, coll, %{foo: 43}) |> Enum.to_list()
    assert [_] = Mongo.find(c.pid, coll, %{foo: 1}) |> Enum.to_list()
  end

  test "update_one!", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1, upserted_ids: []} = Mongo.update_one!(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert %Mongo.UpdateResult{acknowledged: false, matched_count: 0, modified_count: 0, upserted_ids: []} == Mongo.update_one!(c.pid, coll, %{foo: 42}, %{}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.update_one!(c.pid, coll, %{foo: 0}, %{"$set": %{_id: 0}})
    end
  end

  test "update_many", c do
    coll = unique_name()

    assert_raise ArgumentError, fn -> Mongo.update_many(c.pid, coll, %{foo: 42}, %{foo: 0}) end

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{foo: 43}])

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 2, modified_count: 2, upserted_ids: []}} = Mongo.update_many(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert [_, _] = Mongo.find(c.pid, coll, %{foo: 0}) |> Enum.to_list()
    assert [] = Mongo.find(c.pid, coll, %{foo: 42}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 0, upserted_ids: [id]}} = Mongo.update_many(c.pid, coll, %{foo: 50}, %{"$set": %{foo: 0}}, upsert: true)
    assert [_] = Mongo.find(c.pid, coll, %{_id: id}) |> Enum.to_list()

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 1, upserted_ids: []}} = Mongo.update_many(c.pid, coll, %{foo: 43}, %{"$set": %{foo: 1}}, upsert: true)
    assert [] = Mongo.find(c.pid, coll, %{foo: 43}) |> Enum.to_list()
    assert [_] = Mongo.find(c.pid, coll, %{foo: 1}) |> Enum.to_list()
  end

  test "update_many!", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{_id: 1}])

    assert %Mongo.UpdateResult{acknowledged: true, matched_count: 2, modified_count: 2, upserted_ids: []} = Mongo.update_many!(c.pid, coll, %{foo: 42}, %{"$set": %{foo: 0}})

    assert %Mongo.UpdateResult{acknowledged: false, matched_count: 0, modified_count: 0, upserted_ids: []} == Mongo.update_many!(c.pid, coll, %{foo: 0}, %{}, w: 0)

    assert_raise Mongo.WriteError, fn ->
      Mongo.update_many!(c.pid, coll, %{foo: 0}, %{"$set": %{_id: 1}})
    end
  end

  test "update", c do
    coll = unique_name()

    assert {:ok, _} = Mongo.insert_many(c.pid, coll, [%{foo: 42}, %{foo: 42}, %{_id: 1}])

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 2, modified_count: 2, upserted_ids: []}} = Mongo.update(c.pid, coll, q: %{foo: 42}, update: %{"$set": %{foo: 0}}, multi: true)

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 0, modified_count: 0, upserted_ids: []}} == Mongo.update(c.pid, coll, [query: %{foo: 0}, update: %{}], w: 0)

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 1, modified_count: 0, upserted_ids: [%BSON.ObjectId{}]}} = Mongo.update(c.pid, coll, query: %{foo: 100}, update: %{foo: 24, flag: "new"}, upsert: true)

    assert {:ok, %Mongo.UpdateResult{acknowledged: true, matched_count: 2, modified_count: 1, upserted_ids: [%BSON.ObjectId{}]}} =
             Mongo.update(c.pid, coll, [[q: %{foo: 24}, update: %{flag: "old"}], [q: %{foo: 99}, update: %{luftballons: "yes"}, upsert: true]])

    # message: "Write batch sizes must be between 1 and 100000. Got 0 operations."
    assert {:error, %Mongo.Error{code: 16}} = Mongo.update(c.pid, coll, [])
  end

  # issue #19
  # test "correctly pass options to cursor", c do
  #  assert %Mongo.AggregationCursor{opts: [slave_ok: true, no_cursor_timeout: true], coll: "coll"} =
  #           Mongo.find(c.pid, "coll", %{}, skip: 10, cursor_timeout: false)
  # end

  # issue #220
  @tag :mongo_3_4
  test "correctly query NumberDecimal", c do
    coll = "number_decimal_test"

    Mongo.delete_many(c.pid, coll, %{})

    values =
      [
        %Decimal{coef: :NaN},
        %Decimal{sign: -1, coef: :inf},
        %Decimal{coef: :inf},
        %Decimal{coef: 0, exp: -611},
        %Decimal{sign: -1, coef: 0, exp: -1},
        %Decimal{coef: 1, exp: 3},
        %Decimal{coef: 1234, exp: -6},
        %Decimal{coef: 123_400_000, exp: -11},
        %Decimal{coef: 1_234_567_890_123_456_789_012_345_678_901_234, exp: -34},
        %Decimal{coef: 1_234_567_890_123_456_789_012_345_678_901_234, exp: 0},
        %Decimal{coef: 9_999_999_999_999_999_999_999_999_999_999_999, exp: -6176},
        %Decimal{coef: 1, exp: -6176},
        %Decimal{sign: -1, coef: 1, exp: -6176}
      ]
      |> Enum.with_index()

    Enum.each(values, fn {dec, i} -> Mongo.insert_one(c.pid, coll, %{number: dec, index: i}) end)

    Enum.each(values, fn {dec, i} ->
      assert %{"number" => ^dec} = Mongo.find(c.pid, coll, %{index: i}, limit: 1) |> Enum.to_list() |> List.first()
    end)
  end

  test "access multiple databases", c do
    coll = unique_name()

    Mongo.delete_many(c.pid, coll, %{}, database: "mongodb_test2")
    assert {:ok, _} = Mongo.insert_one(c.pid, coll, %{foo: 42}, database: "mongodb_test2", verbose: true)

    assert {:ok, 1} = Mongo.count(c.pid, coll, %{}, database: "mongodb_test2", verbose: true)
    assert {:ok, 0} = Mongo.count(c.pid, coll, %{}, verbose: true)
  end

  test "save struct", c do
    coll = unique_name()

    value = %TestUser{}
    {:ok, %Mongo.InsertOneResult{inserted_id: id}} = Mongo.insert_one(c.pid, coll, value)
    assert id != nil

    user =
      Mongo.find_one(c.pid, coll, %{_id: id})
      |> Enum.into(%{}, fn {key, val} -> {String.to_atom(key), val} end)

    user = struct(TestUser, user)

    assert value == user
  end

  test "causal consistency", %{pid: top} do
    coll = unique_collection()
    Mongo.drop_collection(top, coll, w: 3)
    Mongo.create(top, coll, w: 3)

    docs = Stream.cycle([%{foo: 10, name: "Zorro"}]) |> Enum.take(100)

    indexes = [
      [key: [foo: 1, name: 1], name: "foo_index"]
    ]

    assert :ok = Mongo.create_indexes(top, coll, indexes)

    prefs = %{mode: :secondary}

    assert %{"name" => "Oskar"} =
             Mongo.causal_consistency(top, fn ->
               assert {:ok, _} = Mongo.insert_many(top, coll, docs, w: :majority)
               assert {:ok, _} = Mongo.insert_one(top, coll, %{name: "Oskar"}, w: :majority)
               Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs, read_concern: %{level: :majority}) |> Map.take(["name"])
             end)
  end

  @tag :rs_required
  test "nested transaction", %{pid: top} do
    coll = unique_collection()
    Mongo.drop_collection(top, coll, w: 3)
    Mongo.create(top, coll, w: 3)

    assert :error =
             Mongo.transaction(top, fn _opts ->
               Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
               test_insert_1(top, coll)
               test_insert_2(top, coll)
               :error
             end)

    assert nil == Mongo.find_one(top, coll, %{name: "Tom"})
    assert nil == Mongo.find_one(top, coll, %{name: "Greta"})
    assert nil == Mongo.find_one(top, coll, %{name: "Oskar"})

    assert {:error, %RuntimeError{message: "Error"}} =
             Mongo.transaction(top, fn ->
               Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
               test_insert_1(top, coll)
               test_insert_3(top, coll)
               :not_returning
             end)

    assert nil == Mongo.find_one(top, coll, %{name: "Tom"})
    assert nil == Mongo.find_one(top, coll, %{name: "Greta"})
    assert nil == Mongo.find_one(top, coll, %{name: "Oskar"})

    assert {:error, %Mongo.Error{message: "Aborting transaction, reason :many_reasons"}} =
             Mongo.transaction(top, fn ->
               Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
               test_insert_1(top, coll)
               test_insert_4(top, coll)
               :ok
             end)

    assert nil == Mongo.find_one(top, coll, %{name: "Tom"})
    assert nil == Mongo.find_one(top, coll, %{name: "Greta"})
    assert nil == Mongo.find_one(top, coll, %{name: "Oskar"})

    assert :ok =
             Mongo.transaction(top, fn ->
               Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
               test_insert_1(top, coll)
               test_insert_2(top, coll)
               :ok
             end)

    assert %{"age" => 13, "name" => "Tom"} = Mongo.find_one(top, coll, %{name: "Tom"})
    assert %{"age" => 11, "name" => "Greta"} = Mongo.find_one(top, coll, %{name: "Greta"})
    assert %{"age" => 14, "name" => "Oskar"} = Mongo.find_one(top, coll, %{name: "Oskar"})
  end

  def test_insert_1(top, coll) do
    Mongo.insert_one(top, coll, %{name: "Greta", age: 11})
  end

  def test_insert_2(top, coll) do
    Mongo.transaction(top, fn _opts ->
      Mongo.insert_one(top, coll, %{name: "Oskar", age: 14})
      :ok
    end)
  end

  def test_insert_3(top, coll) do
    Mongo.transaction(top, fn _opts ->
      Mongo.insert_one(top, coll, %{name: "Fass", age: 15})
      raise "Error"
    end)
  end

  def test_insert_4(_top, _coll) do
    Mongo.abort_transaction(:many_reasons)
  end
end
