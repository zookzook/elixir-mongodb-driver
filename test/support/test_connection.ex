defmodule Mongo.TestConnection do
  @seeds ["127.0.0.1:27017", "127.0.0.1:27018", "127.0.0.1:27019"]

  def connect() do
    Mongo.start_link(database: "mongodb_test", seeds: @seeds, show_sensitive_data_on_connection_error: true)
  end
end
