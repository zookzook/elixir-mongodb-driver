defmodule Mongo.Migration do
  @moduledoc false
  use Mongo.Collection

  def migrate() do
    with :locked <- lock() do
      migration_files!()
      |> compile_migrations()
      |> Enum.each(fn {mod, version} -> run_up(version, mod) end)

      unlock()
    end
  rescue
    _ ->
      unlock()
  end

  def drop() do
    with :locked <- lock() do
      migration_files!()
      |> compile_migrations()
      |> Enum.reverse()
      |> Enum.each(fn {mod, version} -> run_down(version, mod) end)

      unlock()
    end
  rescue
    _ ->
      unlock()
  end

  def lock() do
    topology = get_config()[:topology]
    collection = get_config()[:collection]
    query = %{_id: "lock", used: false}
    set = %{"$set": %{used: true}}

    case Mongo.update_one(topology, collection, query, set, upsert: true) do
      {:ok, %{modified_count: 1}} ->
        IO.puts("🔒 #{collection} locked")
        :locked

      {:ok, %{upserted_ids: ["lock"]}} ->
        IO.puts("🔒 #{collection} locked")
        :locked

      _other ->
        {:error, :already_locked}
    end
  end

  def unlock() do
    topology = get_config()[:topology]
    collection = get_config()[:collection]
    query = %{_id: "lock", used: true}
    set = %{"$set": %{used: false}}

    case Mongo.update_one(topology, collection, query, set) do
      {:ok, %{modified_count: 1}} ->
        IO.puts("🔓 #{collection} unlocked")
        :unlocked

      _other ->
        {:error, :not_locked}
    end
  end

  defp run_up(version, mod) do
    topology = get_config()[:topology]
    collection = get_config()[:collection]

    case Mongo.find_one(topology, collection, %{version: version}) do
      nil ->
        mod.up()
        Mongo.insert_one(topology, collection, %{version: version})
        IO.puts("⚡️ Successfully migrated #{mod}")

      _other ->
        :noop
    end
  rescue
    e ->
      IO.puts("🚨 Error when migrating #{mod}:")
      IO.puts(Exception.format(:error, e, __STACKTRACE__))
      reraise e, __STACKTRACE__
  end

  defp run_down(version, mod) do
    topology = get_config()[:topology]
    collection = get_config()[:collection]

    case Mongo.find_one(topology, collection, %{version: version}) do
      %{"version" => _version} ->
        mod.down()
        Mongo.delete_one(topology, collection, %{version: version})
        IO.puts("💥 Successfully dropped #{mod}")

      _other ->
        :noop
    end
  rescue
    e ->
      IO.puts("🚨 Error when dropping #{mod}:")
      IO.puts(Exception.format(:error, e, __STACKTRACE__))
      reraise e, __STACKTRACE__
  end

  def get_config() do
    defaults = [topology: :mongo, collection: "migrations", path: "mongo/migrations", otp_app: :mongodb_driver]
    Keyword.merge(defaults, Application.get_env(:mongodb_driver, :migration, []))
  end

  def migration_file_path() do
    path = get_config()[:path]
    otp_app = get_config()[:otp_app]
    Path.join([:code.priv_dir(otp_app), path])
  end

  defp migration_files!() do
    case File.ls(migration_file_path()) do
      {:ok, files} -> Enum.sort(files)
      {:error, _} -> raise "Could not find migrations file path"
    end
  end

  defp compile_migrations(files) do
    Enum.map(files, fn file ->
      mod =
        (migration_file_path() <> "/" <> file)
        |> Code.compile_file()
        |> Enum.map(&elem(&1, 0))
        |> List.first()

      version =
        ~r/[0-9]/
        |> Regex.scan(file)
        |> Enum.join()
        |> String.to_integer()

      {mod, version}
    end)
  end
end
