defmodule Mongo.GridFs.Bucket do
  @moduledoc false

  alias Mongo.GridFs.Bucket
  alias BSON.ObjectId

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

  @doc """
  Returns the collection name for the files collection, default is fs.files.
  """
  def files_collection_name(%Bucket{name: fs}), do: "#{fs}.files"

  @doc """
  Returns the collection name for the chunks collection, default is fs.chunks.
  """
  def chunks_collection_name(%Bucket{name: fs}), do: "#{fs}.chunks"


  @doc """
  Renames the stored file with the specified @id.
  """
  def rename(%Bucket{conn: conn} = bucket, id, new_filename) do
    query = %{_id: id}
    update = %{ "$set" => %{filename: new_filename}}
    with collection <- files_collection_name(bucket) do
         {:ok, _} = Mongo.find_one_and_update(conn,collection,query,update)
    end
  end

  @doc """
  Given a @id, delete this stored file’s files collection document and
  associated chunks from a GridFS bucket.
  """
  def delete(%Bucket{} = bucket, file_id) when is_binary(file_id) do
    delete(bucket,ObjectId.decode!(file_id))
  end

  def delete(%Bucket{conn: conn} = bucket, %BSON.ObjectId{} = oid) do
    # first delete files document
    collection = files_collection_name(bucket)
    {:ok, %Mongo.DeleteResult{deleted_count: _}} = Mongo.delete_one(conn, collection, %{_id: oid})

    # then delete all chunk documents
    collection = chunks_collection_name(bucket)
    {:ok, %Mongo.DeleteResult{deleted_count: _}} = Mongo.delete_many(conn, collection, %{files_id: oid})
  end

  def find_one_file( %Bucket{} = bucket, file_id) when is_binary(file_id) do
    find_one_file(bucket,ObjectId.decode!(file_id))
  end

  def find_one_file( %Bucket{conn: conn} = bucket, %BSON.ObjectId{} = oid ) do
    collection = files_collection_name(bucket)
    conn |> Mongo.find_one(collection, %{"_id" => oid} )
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

