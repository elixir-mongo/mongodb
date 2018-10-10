defmodule Mongo.GridFs.Download do
  @moduledoc false

  alias BSON.ObjectId
  alias Mongo.GridFs.Bucket

  @doc """
  Opens a Stream from which the application can read the contents of the stored file
  specified by fileId.
  Returns a Stream.
  """
  def open_download_stream( bucket, file_id ) when is_binary(file_id) do
    bucket
    |> find_one_file( %{ "_id" => ObjectId.decode!(file_id)} )
  end

  @doc """
  Same as above, accepting an OID
  """
  def open_download_stream( bucket, %BSON.ObjectId{} = oid ) do
    bucket |> find_one_file( %{ "_id" => oid } )
  end

  @doc """
  Same as above, accepting an fs.files map
  """
  def open_download_stream( bucket, %{ "length" => _, "_id" => _} = file ) do
    file |> stream_chunk( bucket )
  end

  @doc """
  Same as above, but returns also the file document.
  """
  def find_and_stream( %Bucket{conn: conn} = bucket, file_id ) when is_binary(file_id) do
    file = conn |> Mongo.find_one( "fs.files", %{ "_id" => ObjectId.decode!(file_id)} )
    {file |> stream_chunk(bucket), file}
  end

  ##
  # finds the file map and if found the chunks are streamed
  #
  defp find_one_file( %Bucket{conn: conn} = bucket, query ) do
    conn
    |> Mongo.find_one( "fs.files", query )
    |> stream_chunk( bucket )
  end

  ##
  # In case that the file map is nil we return :error
  #
  defp stream_chunk( nil, _bucket ), do: {:error, :not_found}

  ##
  # However, when downloading a zero length stored file the driver MUST NOT issue a query against the chunks
  # collection, since that query is not necessary. For a zero length file, drivers return either an empty
  # stream or send nothing to the provided stream (depending on the download method).
  ##
  defp stream_chunk( %{ "length" => 0 }, _bucket ), do: {:error, :length_is_zero}

  ##
  # Streaming the chunks with `file_id` sorted ascending by n
  #
  defp stream_chunk( %{ "_id" => id }, %Bucket{conn: conn} ) do
    stream = conn
             |> Mongo.find( "fs.chunks", %{ files_id: id }, sort: [n: 1] )
             |> Stream.map( fn map -> map["data"].binary end )
    {:ok, stream}
  end

  ##
  # catch up for other cases
  #
  defp stream_chunk( _, _bucket ), do: {:error, :unknown}

end
