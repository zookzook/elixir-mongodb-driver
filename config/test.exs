import Config

config :mongodb_driver, Mongo.RepoTest.MyRepo,
  url: "mongodb://127.0.0.1:27017/mongodb_test",
  show_sensitive_data_on_connection_error: true

config :mongodb_driver,
  log: false
