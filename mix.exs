defmodule Mongodb.Mixfile do
  use Mix.Project

  @version "0.2.0"

  def project do
    [app: :mongodb,
     version: @version,
     elixir: "~> 1.2",
     name: "Mongodb",
     deps: deps(),
     docs: docs(),
     description: description(),
     package: package()]
  end

  def application do
    [applications: [:logger, :connection, :db_connection],
     mod: {Mongo.App, []},
     env: []]
  end

  defp deps do
    [{:connection,    "~> 1.0"},
     {:db_connection, "~> 1.0"},
     {:ex_doc,        ">= 0.0.0", only: :dev},
     {:earmark,       ">= 0.0.0", only: :dev}]
  end

  defp docs do
    [main: "readme",
     extras: ["README.md"],
     source_ref: "v#{@version}",
     source_url: "https://github.com/ericmj/mongodb"]
  end

  defp description do
    "MongoDB driver for Elixir"
  end

  defp package do
    [maintainers: ["Eric Meadows-Jönsson"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/ericmj/mongodb"}]
  end
end
