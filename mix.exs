defmodule Mongodb.Mixfile do
  use Mix.Project

  @version "1.0.0-beta.1"

  def project do
    [
      app: :mongodb,
      version: @version,
      elixirc_paths: elixirc_paths(Mix.env()),
      elixir: "~> 1.5",
      name: "Mongodb",
      deps: deps(),
      docs: docs(),
      description: description(),
      package: package(),
      dialyzer: dialyzer(),
      consolidate_protocols: Mix.env() != :test,
      test_coverage: [summary: [threshold: 70]]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      mod: {Mongo.App, []},
      env: [],
      extra_applications: [:crypto, :logger, :ssl],
      registered: [
        Mongo.PBKDF2Cache,
        Mongo.Session.Supervisor,
        Mongo.Events,
        Mongo.IdServer,
        Mongo.SessionPool
      ]
    ]
  end

  defp deps do
    [
      {:credo, "~> 1.5.6", only: [:dev, :test], runtime: false},
      {:db_connection, "~> 2.4.0"},
      {:decimal, "~> 2.0.0"},
      {:dialyxir, "~> 1.1.0", only: :dev, runtime: false},
      {:earmark, ">= 0.0.0", only: :dev},
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:jason, "~> 1.2.2", only: [:dev, :test]}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "v#{@version}",
      source_url: "https://github.com/elixir-mongo/mongodb"
    ]
  end

  defp description do
    "MongoDB driver for Elixir"
  end

  defp package do
    [
      maintainers: ["Eric Meadows-Jönsson", "Justin Wood"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/elixir-mongo/mongodb"}
    ]
  end

  # Configures dialyzer.
  #
  # The `dialyzer.plt` file takes a long time to generate first time round, so we store it in a
  # custom location where it can then be easily cached during CI.
  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
    ]
  end
end
