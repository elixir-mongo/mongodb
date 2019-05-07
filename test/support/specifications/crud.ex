defmodule Mongo.Specification.CRUD do
  @moduledoc false

  defmacro create_tests(json) do
    quote bind_quoted: [json: json] do
      Enum.map(json["tests"], fn t ->
        @tag :specification
        test t["description"], %{mongo: mongo, collection: collection} do
          test_json = unquote(Macro.escape(t))
          json = unquote(Macro.escape(json))

          if min_server_version?(json["minServerVersion"]) do
            data = json["data"]
            operation = test_json["operation"]
            outcome = test_json["outcome"]

            if data != [] do
              Mongo.insert_many!(mongo, collection, data)
            end

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
  end
end
