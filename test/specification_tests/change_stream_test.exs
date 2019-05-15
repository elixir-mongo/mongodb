defmodule Mongo.SpecificationTests.ChangeStreamTest do
  use Mongo.SpecificationCase
  import Mongo.Specification.ChangeStream.Helpers
  require Mongo.Specification.ChangeStream

  # setup_all do
  #   {:ok, pid} = Mongo.start_link(database: "mongodb_test")

  #   %{global_mongo: pid}
  # end

  # Enum.map(@change_stream_tests, fn file ->
  #   json = file |> File.read!() |> Jason.decode!()
  #   [file_no_suffix, _suffix] =
  #     file
  #     |> String.split("/")
  #     |> List.last()
  #     |> String.split(".")

  #   describe file do
  #     setup %{global_mongo: global_mongo} do
  #       # collection = unquote(Macro.escape(file_no_suffix))
  #       collection = json["collection_name"]
  #       database = json["database_name"]
  #       {:ok, pid} = Mongo.start_link(database: )
  #       %{collection: collection}
  #     end

  #     Mongo.Specification.ChangeStream.create_tests(json)
  #   end
  # end)
end
