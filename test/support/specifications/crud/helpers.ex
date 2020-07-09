defmodule Mongo.Specification.CRUD.Helpers do
  defp atomize_keys(map) do
    Enum.map(map, fn {key, value} ->
      {String.to_existing_atom(key), value}
    end)
  end

  def find(pid, collection, arguments) do
    filter = arguments["filter"]

    opts =
      arguments
      |> Map.drop(["filter"])
      |> atomize_keys()

    pid |> Mongo.find(collection, filter, opts) |> Enum.to_list()
  end

  def distinct(pid, collection, arguments) do
    field_name = arguments["fieldName"]
    filter = arguments["filter"] || %{}

    opts =
      arguments
      |> Map.drop(["fieldName", "filter"])
      |> atomize_keys()

    {:ok, values} = Mongo.distinct(pid, collection, field_name, filter, opts)
    values
  end

  def estimated_document_count(pid, collection, arguments) do
    opts = atomize_keys(arguments)

    {:ok, result} = Mongo.estimated_document_count(pid, collection, opts)
    result
  end

  def count_documents(pid, collection, arguments) do
    filter = arguments["filter"]

    opts =
      arguments
      |> Map.drop(["filter"])
      |> atomize_keys()

    {:ok, result} = Mongo.count_documents(pid, collection, filter, opts)
    result
  end

  def count(pid, collection, arguments) do
    filter = arguments["filter"]

    opts =
      arguments
      |> Map.drop(["filter"])
      |> atomize_keys()

    {:ok, result} = Mongo.count(pid, collection, filter, opts)
    result
  end

  def aggregate(pid, collection, arguments) do
    pipeline = arguments["pipeline"]

    opts =
      arguments
      |> Map.drop(["pipeline"])
      |> atomize_keys()

    {:ok, cursor} = pid |> Mongo.aggregate(collection, pipeline, opts) |> Enum.to_list()
    cursor
  end

  def match_operation_result?(expected, actual) do
    actual == [] || expected == actual
  end

  def operation_name("estimatedDocumentCount"), do: :estimated_document_count
  def operation_name("countDocuments"), do: :count_documents
  def operation_name(name), do: String.to_existing_atom(name)
end
