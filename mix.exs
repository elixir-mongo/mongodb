defmodule Mongodb.Mixfile do
  use Mix.Project

  def project do
    [app: :mongodb,
     version: "0.1.1",
     elixir: "~> 1.0",
     deps: deps,
     name: "Mongodb",
     source_url: "https://github.com/ericmj/mongodb",
     docs: fn ->
       {ref, 0} = System.cmd("git", ["rev-parse", "--verify", "--quiet", "HEAD"])
       [source_ref: ref, main: "README", readme: "README.md"]
     end,
     description: description,
     package: package]
  end

  def application do
    [applications: [:logger, :connection],
     mod: {Mongo, []},
     env: []]
  end

  defp deps do
    [{:connection, "~> 1.0"},
     {:poolboy,    "~> 1.5", optional: true},
     {:ex_doc,     ">= 0.0.0", only: :docs},
     {:earmark,    ">= 0.0.0", only: :docs},
     {:inch_ex,    ">= 0.0.0", only: :docs}]
  end

  defp description do
    "MongoDB driver for Elixir."
  end

  defp package do
    [maintainers: ["Eric Meadows-Jönsson"],
     licenses: ["Apache 2.0"],
     links: %{"Github" => "https://github.com/ericmj/mongodb"}]
  end
end
