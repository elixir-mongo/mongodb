defmodule Mongo.SpecificationTests.CRUDTest do
  use Mongo.SpecificationCase

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
        :ok
      end

      Enum.map(json["tests"], fn t ->
        @tag :specification
        test t["description"], %{mongo: mongo} do
          test_json = unquote(Macro.escape(t))
          json = unquote(Macro.escape(json))
          file_no_suffix = unquote(Macro.escape(file_no_suffix))

          min_server_version =
            json["minServerVersion"] <> ".0"
            |> String.split(".")
            |> Enum.map(&elem(Integer.parse(&1), 0))
            |> List.to_tuple()

          if mongo_version() >= min_server_version do
            data = json["data"]
            collection = file_no_suffix
            operation = test_json["operation"]
            outcome = test_json["outcome"]

            Mongo.insert_many!(mongo, collection, data)

            name = String.to_existing_atom(Map.get(operation, "name"))
            pipeline = operation["arguments"]["pipeline"]
            rest =
              operation["arguments"]
              |> Map.drop(["pipeline"])
              |> Enum.map(fn {key, value} ->
              {String.to_existing_atom(key), value}
            end)

              expected = outcome["result"]
              applied =
                Mongo
                |> apply(name, [mongo, collection, pipeline, rest])
                |> Enum.to_list

              actual = if outcome["collection"] do
                outcome_collection = outcome["collection"]["name"]
                Mongo.find(mongo, outcome_collection, %{}) |> Enum.to_list
              else
                applied
              end

              assert ^expected = actual
          end
        end
      end)
    end
  end)
end
