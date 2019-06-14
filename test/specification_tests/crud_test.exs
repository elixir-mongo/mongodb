defmodule Mongo.SpecificationTests.CRUDTest do
  use Mongo.SpecificationCase
  import Mongo.Specification.CRUD.Helpers
  require Mongo.Specification.CRUD

  def min_server_version?(nil), do: true

  def min_server_version?(number) do
    min_server_version =
      (number <> ".0")
      |> String.split(".")
      |> Enum.map(&elem(Integer.parse(&1), 0))
      |> List.to_tuple()

    mongo_version() >= min_server_version
  end

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
