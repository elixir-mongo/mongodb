defmodule Mongo.ReadResult do
  defstruct [
    :from,
    :num,
    :docs,
    :cursor_id
  ]
end
