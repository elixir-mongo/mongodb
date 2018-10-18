defmodule Mongo.GridFs.Download do
  @moduledoc """
  The main entry point for downloading files from the grid-fs specified by the bucket struct.
  """

  alias BSON.ObjectId
  alias Mongo.GridFs.Bucket

  @type result :: {:error, :unknown} | {:error, :length_is_zero} | {:error, :not_found} | {:ok, Mongo.cursor}

  @doc """
  Opens a Stream from which the application can read the contents of the stored file
  specified by fileId. The fileId can be a string, an ObjectId or just a map with the
  keys `length` and `_id`. In case of the map the function tries to stream the chunks
  described by the `length` and the `_id` values.

  Returns a Stream.
  """
  @spec open_download_stream(Bucket.t, String.t | BSON.ObjectId.t | map()) :: result
  def open_download_stream(bucket, file_id) when is_binary(file_id) do
    find_one_file(bucket, %{"_id" => ObjectId.decode!(file_id)})
  end

  def open_download_stream(bucket, %BSON.ObjectId{} = oid) do
     find_one_file(bucket, %{"_id" => oid})
  end

  def open_download_stream(bucket, %{"length" => _, "_id" => _} = file) do
    stream_chunk(file, bucket)
  end

  @doc """
  Same as above, but returns also the file document.
  """
  @spec find_and_stream(Bucket.t, String.t) :: {result, BSON.document}
  def find_and_stream(%Bucket{topology_pid: topology_pid} = bucket, file_id) when is_binary(file_id) do
    file = Mongo.find_one(topology_pid, "fs.files", %{"_id" => ObjectId.decode!(file_id)})
    {stream_chunk(file, bucket), file}
  end

  ##
  # finds the file map and if found the chunks are streamed
  #
  defp find_one_file(%Bucket{topology_pid: topology_pid} = bucket, query) do
    topology_pid
    |> Mongo.find_one("fs.files", query)
    |> stream_chunk(bucket)
  end

  ##
  # In case that the file map is nil we return :error
  #
  defp stream_chunk(nil, _bucket), do: {:error, :not_found}

  ##
  # However, when downloading a zero length stored file the driver MUST NOT issue a query against the chunks
  # collection, since that query is not necessary. For a zero length file, drivers return either an empty
  # stream or send nothing to the provided stream (depending on the download method).
  ##
  defp stream_chunk(%{"length" => 0}, _bucket), do: {:error, :length_is_zero}

  ##
  # Streaming the chunks with `file_id` sorted ascending by n
  #
  defp stream_chunk(%{"_id" => id}, %Bucket{topology_pid: topology_pid}) do
    stream = topology_pid
             |> Mongo.find("fs.chunks", %{files_id: id}, sort: [n: 1])
             |> Stream.map(fn map -> map["data"] end)
    {:ok, stream}
  end

  ##
  # catch up for other cases
  #
  defp stream_chunk(_, _bucket), do: {:error, :unknown}

end
