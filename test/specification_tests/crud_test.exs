defmodule Mongo.SpecificationTests.CRUDTest do
  use Mongo.SpecificationCase
  import Mongo.Specification.CRUD.Helpers

  def min_server_version?(nil), do: true
  def min_server_version?(number) do
    min_server_version =
      number <> ".0"
      |> String.split(".")
      |> Enum.map(&elem(Integer.parse(&1), 0))
      |> List.to_tuple()

    mongo_version() >= min_server_version
  end

  setup_all do
    {:ok, pid} = Mongo.start_link(database: "mongodb_test")

    %{mongo: pid}
  end

  Enum.map(@crud_tests, fn file ->
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

      Enum.map(json["tests"], fn t ->
        @tag :specification
        test t["description"], %{mongo: mongo, collection: collection} do
          test_json = unquote(Macro.escape(t))
          json = unquote(Macro.escape(json))

          if min_server_version?(json["minServerVersion"]) do
            data = json["data"]
            operation = test_json["operation"]
            outcome = test_json["outcome"]

            Mongo.insert_many!(mongo, collection, data)

            name = operation_name(operation["name"])
            arguments = operation["arguments"]

            expected = outcome["result"]
            actual = apply(Mongo.Specification.CRUD.Helpers, name, [mongo, collection, arguments])

            assert match_operation_result?(expected, actual)

            if outcome["collection"] do
              data =
                mongo
                |> Mongo.find(outcome["collection"]["name"], %{})
                |> Enum.to_list
              assert ^data = outcome["collection"]["data"]
            end
          end
        end
      end)
    end
  end)
end
