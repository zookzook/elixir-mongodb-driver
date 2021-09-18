defmodule Reader do

  require Logger

  ##
  # see https://github.com/zookzook/elixir-mongodb-driver/issues/63 for more information
  #
  # 1. start a replica set and call the Reader.test()
  # 2. go to the primary db and call db.adminCommand({replSetStepDown: 30})
  # 3. check the log to see the error message only one time
  ##
  def start_link(conn) do
    Logger.info("starting reader")

    Task.start_link(fn -> read(conn, false) end)
  end

  defp read(conn, error) do

    if error do
      Logger.info("Called with error")
    end

    # Gets an enumerable cursor for the results
    cursor = Mongo.find(conn, "data", %{})

    error = case cursor do
      {:error, error} ->
        Logger.info("Error: #{inspect error}")
        true

      _ ->
        cursor
        |> Enum.to_list()
        |> Enum.count()
        false
    end

    read(conn, error)
  end

  def test() do
    {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017,localhost:27018,localhost:27019/load?replicaSet=rs_1")

    Enum.map(1..10_000, fn counter -> Mongo.insert_one(conn, "data", %{counter: counter}) end)
    Reader.start_link(conn)
  end
end