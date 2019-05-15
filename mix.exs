defmodule Mongodb.Mixfile do
  use Mix.Project

  @version "0.5.1"

  def project do
    [app: :mongodb,
     version: @version,
     elixirc_paths: elixirc_paths(Mix.env),
     elixir: "~> 1.5",
     name: "Mongodb",
     deps: deps(),
     docs: docs(),
     description: description(),
     package: package(),
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

  def application do
    [applications: applications(Mix.env),
     mod: {Mongo.App, []},
     env: []]
  end

  def applications(:test), do: [:logger, :connection, :db_connection]
  def applications(_), do: [:logger, :connection, :db_connection]

  defp deps do
    [
      {:connection,    "~> 1.0"},
      {:db_connection, "~> 2.0"},
      {:decimal,       "~> 1.5"},
      # {:poolboy,       ">= 0.0.0", only: :test},
      {:jason,         "~> 1.0.0", only: :test},
      {:ex_doc,        ">= 0.0.0", only: :dev},
      {:earmark,       ">= 0.0.0", only: :dev},
      # {:dialyxir, "~> 1.0.0-rc.4", only: [:dev], runtime: false}
    ]
  end

  defp docs do
    [main: "readme",
     extras: ["README.md"],
     source_ref: "v#{@version}",
     source_url: "https://github.com/ankhers/mongodb"]
  end

  defp description do
    "MongoDB driver for Elixir"
  end

  defp package do
    [maintainers: ["Eric Meadows-Jönsson", "Justin Wood"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/ankhers/mongodb"}]
  end
end
