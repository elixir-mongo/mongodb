defmodule Mongo.SpecificationTests.CRUDTest do
  use Mongo.SpecificationCase
  import Mongo.Specification.CRUD.Helpers
  require Mongo.Specification.CRUD

  setup_all do
    {:ok, pid} = Mongo.start_link(database: "mongodb_test")

    %{mongo: pid}
  end

  Enum.map(@crud_tests_v1, fn file ->
    json = file |> File.read!() |> Jason.decode!()
    [file_no_suffix, _suffix] =
      file
      |> String.split("/")
      |> List.last()
      |> String.split(".")

    describe file do
      setup %{mongo: mongo} do
        collection = unquote(Macro.escape(file_no_suffix))
        Mongo.delete_many!(mongo, collection, %{})
        %{collection: collection}
      end

      Mongo.Specification.CRUD.create_tests(json)
    end
  end)
end
