defmodule Test.Migrations.FailedMigrationUp do
  def up(opts) do
    Keyword.get(opts, :foo, :foo) + 1
  end

  def down(opts) do
    Keyword.get(opts, :foo, :foo) + 1
  end
end
