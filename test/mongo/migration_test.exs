defmodule Mongo.MigrationTest do
  use CollectionCase
  use Patch

  alias Mongo.Migration

  test "test lock and unlock", %{pid: top} do
    Mongo.drop_collection(top, "migrations")
    Patch.patch(Mongo.Migration, :get_config, fn _ -> [topology: top, collection: "migrations", path: "migrations", otp_app: :mongodb_driver] end)
    assert :locked == Migration.lock()
    assert {:error, :already_locked} == Migration.lock()
    assert :unlocked == Migration.unlock()
    assert {:error, :not_locked} == Migration.unlock()
  end

  test "test lock and unlock with database options", %{pid: top} do
    Mongo.drop_collection(top, "migrations", database: "one")
    Mongo.drop_collection(top, "migrations", database: "two")
    Patch.patch(Mongo.Migration, :get_config, fn _ -> [topology: top, collection: "migrations", path: "migrations", otp_app: :mongodb_driver] end)
    assert :locked == Migration.lock(database: "one")
    assert :locked == Migration.lock(database: "two")
    assert {:error, :already_locked} == Migration.lock(database: "one")
    assert {:error, :already_locked} == Migration.lock(database: "two")
    assert :unlocked == Migration.unlock(database: "one")
    assert :unlocked == Migration.unlock(database: "two")
    assert {:error, :not_locked} == Migration.unlock(database: "one")
    assert {:error, :not_locked} == Migration.unlock(database: "two")
  end

  test "migrate should return :unlocked", %{pid: top} do
    patch(Mongo.Migration, :migration_files!, fn _ -> ["test/data/20220130130208_create_attachments_indexes.exs"] end)
    assert :unlocked == Mongo.Migration.migrate(topology: top)
  end

  test "migrate should return an error", %{pid: top} do
    patch(Mongo.Migration, :migration_files!, fn _ -> ["test/data/20230130130208_create_attachments_indexes.exs"] end)
    assert %ArithmeticError{message: "bad argument in arithmetic expression", __exception__: true} == Mongo.Migration.migrate(topology: top)
  end

  test "drop should return :error", %{pid: top} do
    patch(Mongo.Migration, :migration_files!, fn _ -> ["test/data/20240130130208_create_attachments_indexes.exs"] end)
    patch(Mongo, :find_one, fn _top, _col, _query, _opts -> %{"version" => "123"} end)
    assert %ArithmeticError{message: "bad argument in arithmetic expression", __exception__: true} == Mongo.Migration.drop(topology: top)
  end
end
