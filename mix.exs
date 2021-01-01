defmodule Mongodb.Mixfile do
  use Mix.Project

  @version "0.7.1"

  def project() do
    [app: :mongodb_driver,
     version: @version,
     elixirc_paths: elixirc_paths(Mix.env),
     elixir: "~> 1.8",
     name: "mongodb-driver",
     deps: deps(),
     docs: docs(),
     description: description(),
     package: package(),
     test_coverage: [tool: ExCoveralls],
     preferred_cli_env: [docs: :docs, coveralls: :test, "coveralls.detail": :test, "coveralls.post": :test, "coveralls.html": :test],
     dialyzer: [
       flags: [:underspecs, :unknown, :unmatched_returns],
       plt_add_apps: [:logger, :connection, :db_connection, :mix, :elixir, :ssl, :public_key],
       plt_add_deps: :transitive,
       plt_core_path: "plt_core_path"
     ]
   ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_),     do: ["lib"]

  def application() do
    [applications: applications(Mix.env),
     mod: {Mongo.App, []},
     env: []]
  end

  def applications(:test), do: [:logger, :connection, :db_connection]
  def applications(_), do: [:logger, :connection, :db_connection]

  defp deps() do
    [
      {:db_connection, "~> 2.2.1"},
      {:decimal,       "~> 2.0"},
      {:excoveralls,   "~> 0.12.1", only: :test},
      {:benchee,       "~> 1.0", only: :dev},
      {:jason,         "~> 1.2", only: :test},
      {:ex_doc,        "~> 0.20.1 ", only: :dev},
      {:earmark,       ">= 0.0.0", only: :dev},
      {:dialyxir,      "~> 0.5", only: [:dev], runtime: false}
    ]
  end

  defp docs() do
    [main: "readme",
     extras: ["README.md"],
     source_ref: "#{@version}",
     source_url: "https://github.com/zookzook/elixir-mongodb-driver"]
  end

  defp description() do
    "An alternative MongoDB driver for Elixir"
  end

  defp package() do
    [maintainers: ["Michael Maier"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/zookzook/elixir-mongodb-driver"}]
  end
end
