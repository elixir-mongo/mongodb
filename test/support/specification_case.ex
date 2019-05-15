defmodule Mongo.SpecificationCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      @change_stream_tests Path.wildcard("test/support/change_stream_tests/**/*.json")
      @crud_tests_v1 Path.wildcard("test/support/crud_tests/v1/**/*.json")

      import MongoTest.Case
      import Mongo.SpecificationCase
    end
  end

  # TODO: This should be using a connection and not shell out.
  def mongo_version do
    {string, 0} = System.cmd("mongod", ~w'--version')
    ["db version v" <> version, _] = String.split(string, "\n", parts: 2)

    Mongo.Version.from_string(version)
  end

  def min_server_version?(nil), do: true
  def min_server_version?(number) do
    mongo_version() >= Mongo.Version.from_string(number)
  end
end
