defmodule Mongo.SpecificationCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      @crud_tests Path.wildcard("test/support/crud_tests/**/*.json")

      import MongoTest.Case
    end
  end
end
