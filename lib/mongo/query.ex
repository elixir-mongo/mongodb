defmodule Mongo.Query do
  @moduledoc false

  defstruct action: nil, extra: nil, encoded?: false
end

defimpl DBConnection.Query, for: Mongo.Query do
  import Mongo.Messages, only: [op_reply: 1, op_reply: 2]

  def parse(query, _opts), do: query
  def describe(query, _opts), do: query

  def encode(query, params, _opts) do
    if query.encoded? do
      params
    else
      Enum.map(params, fn
        nil -> ""
        doc -> BSON.Encoder.document(doc)
      end)
    end
  end

  def decode(_query, :ok, _opts),
    do: :ok
  def decode(_query, wire_version, _opts) when is_integer(wire_version),
    do: wire_version
  def decode(_query, op_reply(docs: docs) = reply, _opts),
    do: op_reply(reply, docs: BSON.Decoder.documents(docs))
end
