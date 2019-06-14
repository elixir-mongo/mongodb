defmodule Mongo.SpecificationCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      @crud_tests_v1 Path.wildcard("test/support/crud_tests/v1/**/*.json")

      import MongoTest.Case
      import Mongo.SpecificationCase
    end
  end

  def mongo_version do
    {string, 0} = System.cmd("mongod", ~w'--version')
    ["db version v" <> version, _] = String.split(string, "\n", parts: 2)

    version
    |> String.split(".")
    |> Enum.map(&elem(Integer.parse(&1), 0))
    |> List.to_tuple()
  end
end
