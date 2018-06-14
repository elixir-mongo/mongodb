defmodule Mongo.SpecificationTests.CRUDTest do
  use Mongo.SpecificationCase, async: true

  Enum.map(@crud_tests, fn file ->
    json = file |> File.read!() |> Jason.decode!()

    Enum.map(json["tests"], fn t ->
      @tag :specification
      test t["description"] do
        {:ok, pid} = Mongo.start_link(database: "mongodb_test")
        test_json = unquote(Macro.escape(t))
        json = unquote(Macro.escape(json))

        data = json["data"]
        collection = unique_name()
        operation = test_json["operation"]
        outcome = test_json["outcome"]

        Mongo.insert_many!(pid, collection, data)

        name = String.to_existing_atom(Map.get(operation, "name"))
        pipeline = operation["arguments"]["pipeline"]
        rest =
          operation["arguments"]
          |> Map.drop(["pipeline"])
          |> Enum.map(fn {key, value} ->
            {String.to_existing_atom(key), value}
          end)

        expected = outcome["result"]
        actual =
          Mongo
          |> apply(name, [pid, collection, pipeline, rest])
          |> Enum.to_list()

        assert ^expected = actual
      end
    end)
  end)
end
