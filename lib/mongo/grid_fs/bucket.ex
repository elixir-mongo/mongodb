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

  defstruct name: "fs", chunk_size: 255*1024, topology_pid: nil

  @doc """
  Creates a new Bucket with a existing connection using the default values. It just contains the
  name of the collections (fs) and the chunk size (255KB).

  The bucket checks the index for both collections as well. In case of multiple
  upload or downloads just create only one bucket and resuse it.

  """
  def new( topology_pid, options \\ [] ) do

    Keyword.merge(@defaults, options)
    |> Enum.reduce( %Bucket{topology_pid: topology_pid}, fn {k,v},bucket -> Map.put(bucket, k, v) end)
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
  def rename(%Bucket{topology_pid: topology_pid} = bucket, id, new_filename) do
    query      = %{_id: id}
    update     = %{ "$set" => %{filename: new_filename}}
    collection = files_collection_name(bucket)
    {:ok, _}   = Mongo.find_one_and_update(topology_pid, collection, query, update)
  end

  @doc """
  Given a @id, delete this stored file’s files collection document and
  associated chunks from a GridFS bucket.
  """
  def delete(%Bucket{} = bucket, file_id) when is_binary(file_id) do
    delete(bucket,ObjectId.decode!(file_id))
  end

  def delete(%Bucket{topology_pid: topology_pid} = bucket, %BSON.ObjectId{} = oid) do
    # first delete files document
    collection = files_collection_name(bucket)
    {:ok, %Mongo.DeleteResult{deleted_count: _}} = Mongo.delete_one(topology_pid, collection, %{_id: oid})

    # then delete all chunk documents
    collection = chunks_collection_name(bucket)
    {:ok, %Mongo.DeleteResult{deleted_count: _}} = Mongo.delete_many(topology_pid, collection, %{files_id: oid})
  end

  @doc """
  Drops the files and chunks collections associated with
  this bucket.
  """
  def drop(%Bucket{topology_pid: topology_pid} = bucket) do
    {:ok, _} = Mongo.command(topology_pid, %{drop: files_collection_name(bucket)})
    {:ok, _} = Mongo.command(topology_pid, %{drop: chunks_collection_name(bucket)})
  end

  def find_one_file( %Bucket{} = bucket, file_id) when is_binary(file_id) do
    find_one_file(bucket, ObjectId.decode!(file_id))
  end

  def find_one_file( %Bucket{topology_pid: topology_pid} = bucket, %BSON.ObjectId{} = oid ) do
    collection = files_collection_name(bucket)
    topology_pid |> Mongo.find_one(collection, %{"_id" => oid} )
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
  defp files_collection_empty?( %Bucket{topology_pid: topology_pid} = bucket ) do

    collection = files_collection_name(bucket)

    topology_pid
    |> Mongo.find_one(collection, %{}, projection: %{_id: 1} )
    |> is_nil()

  end

  ##
  # Checks the indexes for the fs.files collection
  #
  defp check_files_index( %Bucket{topology_pid: topology_pid} = bucket ) do

    result = with coll              <- files_collection_name(bucket),
                  {:ok, conn, _, _} <- Mongo.select_server(topology_pid, :read ) do

      aggregation_cursor(conn, "$cmd",  [listIndexes: coll], nil, [] )
      |> Enum.map( fn %{"name" => name } -> name end)
      |> Enum.member?( @files_index_name )
    end

    {bucket, result}
  end

  ##
  # Checks the indexes for the fs.chunks collection
  #
  defp check_chunks_index( %Bucket{topology_pid: topology_pid} = bucket ) do

    result = with coll              <- chunks_collection_name(bucket),
                  {:ok, conn, _, _} <- Mongo.select_server(topology_pid, :read ) do

      aggregation_cursor(conn, "$cmd",  [listIndexes: coll], nil, [] )
      |> Enum.map( fn %{"name" => name } -> name end)
      |> Enum.member?( @chunks_index_name )
    end

    {bucket, result}
  end

  ##
  # Creates the indexes for the fs.chunks collection
  #
  defp create_chunks_index({%Bucket{topology_pid: topology_pid} = bucket, false} ) do

    coll     = chunks_collection_name(bucket)
    cmd      = [createIndexes: coll, indexes: [[key: [files_id: 1, n: 1], name: @chunks_index_name, unique: true]]]
    {:ok, _} = Mongo.command(topology_pid, cmd)

    bucket
  end

  ##
  # index exists, nothing to do
  #
  defp create_chunks_index({bucket, true}), do: bucket

  ##
  # Creates the indexes for the fs.files collection
  #
  defp create_files_index( {%Bucket{topology_pid: topology_pid} = bucket, false} ) do

    coll     = files_collection_name(bucket)
    cmd      = [createIndexes: coll, indexes: [[key: [filename: 1, uploadDate: 1], name: @files_index_name]]]
    {:ok, _} = Mongo.command(topology_pid, cmd)

    bucket
  end

  ##
  # index exists, nothing to do
  #
  defp create_files_index({bucket, true}), do: bucket

  defp aggregation_cursor(conn, coll, query, select, opts) do
    %Mongo.AggregationCursor{
      conn: conn,
      coll: coll,
      query: query,
      select: select,
      opts: opts}
  end


  defimpl Inspect, for: Bucket do

    def inspect( %Bucket{ name: fs, chunk_size: size, topology_pid: topology_pid }, _opts ) do
      "#Bucket(#{fs}, #{size}, topology_pid: #{inspect topology_pid})"
    end

  end

  defimpl String.Chars, for: Bucket do
    def to_string(%Bucket{ name: fs, chunk_size: size, topology_pid: topology_pid }) do
      "#Bucket(#{fs}, #{size}, topology_pid: #{inspect topology_pid})"
    end
  end

end

