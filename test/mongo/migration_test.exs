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
end
