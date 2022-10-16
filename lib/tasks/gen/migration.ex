defmodule Mix.Tasks.Mongo.Gen.Migration do
  @moduledoc false

  use Mix.Task

  import Macro, only: [camelize: 1, underscore: 1]
  import Mix.Generator

  @shortdoc "Generates a new migration for Mongo"

  @spec run([String.t()]) :: integer()
  def run(args) do
    migrations_path = migration_file_path()
    name = List.first(args)
    base_name = "#{underscore(name)}.exs"
    current_timestamp = timestamp()
    file = Path.join(migrations_path, "#{current_timestamp}_#{base_name}")
    unless File.dir?(migrations_path), do: create_directory(migrations_path)
    fuzzy_path = Path.join(migrations_path, "*_#{base_name}")

    if Path.wildcard(fuzzy_path) != [] do
      Mix.raise("Migration can't be created, there is already a migration file with name #{name}.")
    end

    assigns = [mod: Module.concat([Mongo, Migrations, camelize(name)])]
    create_file(file, migration_template(assigns))
    String.to_integer(current_timestamp)
  end

  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: <<?0, ?0 + i>>
  defp pad(i), do: to_string(i)

  def migration_file_path() do
    path = Mongo.Migration.get_config()[:path]
    Path.join(["priv", path])
  end

  embed_template(:migration, """
  defmodule <%= inspect @mod %> do
    def up() do
      # The `up` functions will be executed when running `mix mongo.migrate`
      #
      # indexes = [[key: [files_id: 1, n: 1], name: "files_n_index", unique: true]]
      # Mongo.create_indexes(<%= inspect(Mongo.Migration.get_config()[:topology]) %>, "my_collection", indexes)
    end

    def down() do
      # The `down` functions will be executed when running `mix mongo.drop`
      #
      # Mongo.drop_collection(<%= inspect(Mongo.Migration.get_config()[:topology]) %>, "my_collection")
    end
  end
  """)
end
