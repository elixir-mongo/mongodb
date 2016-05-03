defmodule Mongo.Query do
  @moduledoc false

  defstruct [:action]
end

defimpl DBConnection.Query, for: Mongo.Query do
  def parse(query, _opts), do: query
  def describe(query, _opts), do: query
  def encode(_query, params, _opts), do: params
  def decode(_query, res, _opts), do: res
end
