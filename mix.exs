defmodule Mongodb.Mixfile do
  use Mix.Project

  @source_url "https://github.com/zookzook/elixir-mongodb-driver"
  @version "1.0.2"

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
      consolidate_protocols: Mix.env() != :test
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      applications: applications(Mix.env()),
      env: [],
      extra_applications: [:crypto, :ssl, :eex],
      mod: {Mongo.App, []}
    ]
  end

  def applications(:test), do: [:logger, :connection, :db_connection]
  def applications(_), do: [:logger, :connection, :db_connection]

  defp deps do
    [
      {:telemetry, "~> 1.0"},
      {:db_connection, "~> 2.4.1"},
      {:decimal, "~> 2.0"},
      {:patch, "~> 0.12.0", only: [:dev, :test]},
      {:jason, "~> 1.3", only: [:dev, :test]},
      {:credo, "~> 1.6.1", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
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
      source_ref: @version,
      formatters: ["html"]
    ]
  end

  defp package do
    [
      description: "The MongoDB driver for Elixir",
      maintainers: ["Michael Maier"],
      licenses: ["Apache-2.0"],
      links: %{
        "Changelog" => "https://hexdocs.pm/mongodb_driver/changelog.html",
        "GitHub" => @source_url
      }
    ]
  end
end
