defmodule Mongodb.Mixfile do
  use Mix.Project

  @source_url "https://github.com/zookzook/elixir-mongodb-driver"
  @version "0.8.0"

  def project() do
    [
      app: :mongodb_driver,
      version: @version,
      elixirc_paths: elixirc_paths(Mix.env()),
      elixir: "~> 1.8",
      name: "mongodb-driver",
      deps: deps(),
      docs: docs(),
      package: package(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        docs: :docs,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      dialyzer: [
        flags: [:underspecs, :unknown, :unmatched_returns],
        plt_add_apps: [
          :logger,
          :connection,
          :db_connection,
          :mix,
          :elixir,
          :ssl,
          :public_key
        ],
        plt_add_deps: :transitive,
        plt_core_path: "plt_core_path"
      ],
      consolidate_protocols: Mix.env() != :test
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      applications: applications(Mix.env()),
      env: [],
      extra_applications: [:crypto, :ssl],
      mod: {Mongo.App, []}
    ]
  end

  def applications(:test), do: [:logger, :connection, :db_connection]
  def applications(_), do: [:logger, :connection, :db_connection]

  defp deps do
    [
      {:db_connection, "~> 2.3"},
      {:decimal, "~> 2.0"},
      {:excoveralls, "~> 0.12.1", only: :test},
      {:benchee, "~> 1.0", only: :dev},
      {:jason, "~> 1.2", only: :test},
      {:ex_doc, ">= 0.0.0", only: :docs, runtime: false},
      {:dialyxir, "~> 0.5", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      extras: [
        "CHANGELOG.md": [],
        LICENSE: [title: "License"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      source_url: @source_url,
      source_ref: "#{@version}",
      formatters: ["html"]
    ]
  end

  defp package do
    [
      description: "An alternative MongoDB driver for Elixir",
      maintainers: ["Michael Maier"],
      licenses: ["Apache-2.0"],
      links: %{
        "Changelog" => "https://hexdocs.pm/mongodb_driver/changelog.html",
        "GitHub" => @source_url
      }
    ]
  end
end
