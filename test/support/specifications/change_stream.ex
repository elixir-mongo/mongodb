defmodule Mongo.Specification.ChangeStream do
  @moduledoc false

  defmacro create_tests(json) do
    quote bind_quoted: [json: json] do
      Enum.map(json["tests"], fn t ->
        @tag :specification
        test t["description"], %{mongo: mongo, collection: collection} do
          t = unquote(Macro.escape(t))
          json = unquote(Macro.escape(json))

          if min_server_version?(json["minServerVersion"]) do
          end
        end
      end)
    end
  end
end
