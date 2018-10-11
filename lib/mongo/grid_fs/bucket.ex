defmodule Mongo.GridFs.Bucket do
  @moduledoc false

  alias Mongo.GridFs.Bucket

  ##
  # constants used in this module
  #
  @files_index_name "filename_1_uploadDate_1"
  @chunks_index_name "files_id_1_n_1"
  @defaults [name: "fs", chunk_size: 255*1024]

  defstruct name: "fs", chunk_size: 255*1024, conn: nil

  @doc """
  Creates a new Bucket with a existing connection using the default values. It just contains the
  name of the collections (fs) and the chunk size (255KB).

  The bucket checks the index for both collections as well. In case of multiple
  upload or downloads just create only one bucket and resuse it.

  """
  def new( conn, options \\ [] ) do

    Keyword.merge(@defaults, options)
    |> Enum.reduce( %Bucket{ conn: conn }, fn {k,v},bucket -> Map.put(bucket, k, v) end)
    |> check_indexes

  end

  ##
  # checks and creates indexes for the *.files and *.chunks collections
  #
  defp check_indexes( bucket ) do

    case files_collection_empty?(bucket)do
      true ->
        {bucket, false} |> create_files_index()
        {bucket, false} |> create_chunks_index()

      false ->
        bucket
        |> check_files_index()
        |> create_files_index()
        |> check_chunks_index()
        |> create_chunks_index()
    end

  end

  ##
  # from the specs:
  #
  # To determine whether the files collection is empty drivers SHOULD execute the equivalent of the following shell command:
  #
  # db.fs.files.findOne({}, { _id : 1 })
  #
  defp files_collection_empty?( %Bucket{conn: conn, name: fs } ) do

    collection = "#{fs}.files"

    conn
    |> Mongo.find_one( collection, %{}, projection: %{ _id: 1} )
    |> is_nil()

  end

  ##
  # Checks the indexes for the fs.files collection
  #
  defp check_files_index( %Bucket{conn: conn, name: fs } = bucket ) do

    command = "db.#{fs}.files.getIndexes()"

    {:ok, %{ "retval" => indexes}} = Mongo.command( conn, %{eval: command} )

    result = indexes
             |> Enum.map( fn %{"name" => name } -> name end)
             |> Enum.member?( @files_index_name )

    {bucket, result}
  end

  ##
  # Checks the indexes for the fs.chunks collection
  #
  defp check_chunks_index( %Bucket{conn: conn, name: fs } = bucket ) do

    command = "db.#{fs}.chunks.getIndexes()"

    {:ok, %{ "retval" => indexes}} = Mongo.command( conn, %{eval: command} )

    result = indexes
             |> Enum.map( fn %{"name" => name } -> name end)
             |> Enum.member?( @chunks_index_name )

    {bucket, result}
  end

  ##
  # Creates the indexes for the fs.chunks collection
  #
  defp create_chunks_index({%Bucket{conn: conn, name: fs } = bucket, false} ) do
    command = "db.#{fs}.chunks.createIndex({ files_id : 1, n : 1 }, { unique: true })"
    {:ok, _} = Mongo.command( conn, %{eval: command} )
    bucket
  end

  ##
  # index exists, nothing to do
  #
  defp create_chunks_index({bucket, true}), do: bucket

  ##
  # Creates the indexes for the fs.files collection
  #
  defp create_files_index( {%Bucket{conn: conn, name: fs } = bucket, false} ) do
    command = "db.#{fs}.files.createIndex({ filename : 1, uploadDate : 1 })"
    {:ok, _} = Mongo.command( conn, %{eval: command} )
    bucket
  end

  ##
  # index exists, nothing to do
  #
  defp create_files_index({bucket, true}), do: bucket

  defimpl Inspect, for: Bucket do

    def inspect( %Bucket{ name: fs, chunk_size: size, conn: conn }, _opts ) do
      "#Bucket(#{fs}, #{size}, conn: #{inspect conn})"
    end

  end

  defimpl String.Chars, for: Bucket do
    def to_string(%Bucket{ name: fs, chunk_size: size, conn: conn }) do
      "#Bucket(#{fs}, #{size}, conn: #{inspect conn})"
    end
  end

end

