defmodule Mongo.SpecificationTests.CRUDTest do
  use Mongo.SpecificationCase

  def count_documents(pid, collection, arguments) do
    filter = arguments["filter"]
    opts =
      arguments
      |> Map.drop(["filter"])
      |> Enum.map(fn {key, value} ->
        {String.to_existing_atom(key), value}
      end)

    {:ok, result} = Mongo.count_documents(pid, collection, filter, opts)
    result
  end

  def count(pid, collection, arguments) do
    filter = arguments["filter"]
    opts =
      arguments
      |> Map.drop(["filter"])
      |> Enum.map(fn {key, value} ->
        {String.to_existing_atom(key), value}
      end)

    {:ok, result} = Mongo.count(pid, collection, filter, opts)
    result
  end

  def aggregate(pid, collection, arguments) do
    pipeline = arguments["pipeline"]
    opts =
      arguments
      |> Map.drop(["pipeline"])
      |> Enum.map(fn {key, value} ->
        {String.to_existing_atom(key), value}
      end)

    Mongo.aggregate(pid, collection, pipeline, opts) |> Enum.to_list
  end

  defp match_operation_result?(expected, actual) do
    actual == [] || expected == actual
  end

  defp min_server_version?(nil), do: true
  defp min_server_version?(number) do
    min_server_version =
      number <> ".0"
      |> String.split(".")
      |> Enum.map(&elem(Integer.parse(&1), 0))
      |> List.to_tuple()

    mongo_version() >= min_server_version
  end

  defp operation_name("countDocuments"), do: :count_documents
  defp operation_name(name), do: String.to_existing_atom(name)

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
            actual = apply(__MODULE__, name, [mongo, collection, arguments])

            assert match_operation_result?(expected, actual)

            if outcome["collection"] do
              data = Mongo.find(mongo, outcome["collection"]["name"], %{}) |> Enum.to_list
              assert ^data = outcome["collection"]["data"]
            end
          end
        end
      end)
    end
  end)
end
