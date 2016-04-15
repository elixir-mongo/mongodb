defmodule Mongo.Error do
  @type t :: %Mongo.Error{__exception__: true, message: binary, code: integer}
  defexception [:message, :code]
end
