defmodule Mongodb.Mixfile do
  use Mix.Project

  def project do
    [app: :mongodb,
     version: "0.0.1",
     elixir: "~> 1.0",
     deps: deps]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger, :connection, :pbkdf2],
     mod: {Mongo, []},
     env: []]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    [{:connection, github: "fishcakez/connection"},
     {:pbkdf2, github: "basho/erlang-pbkdf2"},
     {:poolboy, only: [:dev, :test]}]
  end
end
