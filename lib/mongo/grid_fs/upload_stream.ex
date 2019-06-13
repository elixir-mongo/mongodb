defmodule Mongo.GridFs.UploadStream do
  @moduledoc """
  This is the upload stream for save streams into the grid fs.

  First you need to create a bucket. The bucket contains the configuration for the grid fs.

  ## Example:
  streaming the file `./test/data/test.jpg` into the grid fs using the upload-stream

      bucket = Bucket.new( pid )
      upload_stream = Upload.open_upload_stream(bucket, "test.jpg", j: true)

      src_filename = "./test/data/test.jpg"
      File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

  """

  import Record, only: [defrecordp: 2]

  alias Mongo.GridFs.Bucket
  alias Mongo.GridFs.UploadStream

  @type t :: %__MODULE__{
               bucket: Bucket.t,
               id: BSON.ObjectId.t,
               filename: String.t,
               metadata: {BSON.document | nil}
             }
  defstruct bucket: nil, id: nil, filename: nil, metadata: nil

  @doc """
  Creates a new upload stream to insert a file into the grid-fs.
  """
  @spec new(Bucket.t, String.t, BSON.document | nil) :: UploadStream.t
  def new(bucket, filename, metadata \\ nil) do
    %UploadStream{bucket: bucket, filename: filename, id: Mongo.object_id(), metadata: metadata}
  end

  defimpl Collectable, for: UploadStream do

    ##
    # buffer is the current buffer
    # number is the current chunk number
    #
    defrecordp(:state, [:buffer, :number])

    def into(%UploadStream{} = stream) do
      {state(buffer: <<>>, number: 0), make_fun(stream)}
    end

    def make_fun(%UploadStream{bucket: %{chunk_size: chunk_size}} = stream) do
      fn
        # case: buffer is full
        state(buffer: bin) = s, {:cont, x } when byte_size(bin) >= chunk_size ->
          state(buffer: rest, number: next) = write_buffer(stream, s)
          state(buffer: rest <> x, number: next)

        # case: buffer is empty
        state(buffer: bin, number: n), {:cont, x } ->
          state(buffer: bin <> x, number: n)

        # stream stops, write the rest
        state() = s, :done ->
          _ = flush_buffer(stream, s)
          stream

        # steam halts, write the rest
        state() = s, :halt ->
          _ = flush_buffer(stream, s)
          :ok
      end
    end

    ##
    # flushes the buffer and creates the files document
    #
    defp flush_buffer(%UploadStream{bucket: %Bucket{topology_pid: topology_pid, chunk_size: chunk_size, opts: opts} = bucket,
                                    filename: filename,
                                    id: file_id,
                                    metadata: metadata},
                    state(buffer: buffer, number: chunk_number)) do

      collection = Bucket.chunks_collection_name(bucket)
      length = chunk_number * chunk_size + byte_size(buffer)
      insert_one_chunk_document(topology_pid, collection, file_id, buffer, chunk_number, opts)

      collection = Bucket.files_collection_name(bucket)
      insert_one_file_document(topology_pid, collection, file_id, length, chunk_size, filename, metadata, opts)

    end

    ##
    # checks if the buffer is smaller than the chunk-size
    # in this case we do nothing
    #
    defp write_buffer(%UploadStream{bucket: %Bucket{chunk_size: chunk_size}},
                    state(buffer: buffer) = s) when byte_size(buffer) < chunk_size  do
      s
    end

    ##
    # otherwise we
    # write the data to the chunk collections and call the function again with the rest of the buffer
    # for the case that the buffer size is still greater than the chunk size
    #
    defp write_buffer(%UploadStream{bucket: %Bucket{topology_pid: topology_pid, chunk_size: chunk_size, opts: opts} = bucket,
                                   id: file_id} = stream,
                  state(buffer: buffer, number: chunk_number)) do

      collection = Bucket.chunks_collection_name(bucket)
      fun = fn (<<data::bytes-size(chunk_size), rest :: binary>>) ->
        next = insert_one_chunk_document(topology_pid, collection, file_id, data, chunk_number, opts)
        state(buffer: rest, number: next)
      end

      # write the buffer
      new_state = fun.(buffer)

      # try to write the rest of the buffer
      write_buffer(stream, new_state)
    end

    ##
    # inserts one chunk document
    #
    defp insert_one_chunk_document(_topology_pid, _collection, _file_id, data, chunk_number, _opts) when byte_size(data) == 0 do
      chunk_number
    end
    defp insert_one_chunk_document(topology_pid, collection, file_id, binary, chunk_number, opts) do
      bson_binary = %BSON.Binary{binary: binary}
      {:ok, _}    = Mongo.insert_one(topology_pid, collection, %{files_id: file_id, n: chunk_number, data: bson_binary}, opts)
      chunk_number + 1
    end

    ##
    # inserts one file document
    #
    defp insert_one_file_document(topology_pid, collection, file_id, length, chunk_size, filename, metadata, opts) do
      doc = %{_id: file_id, length: length, filename: filename, chunkSize: chunk_size, uploadDate: now(), metadata: metadata}
      {:ok, _} = Mongo.insert_one(topology_pid, collection, doc, opts)
    end

    defp now(), do: DateTime.from_unix!(:os.system_time(), :native)

  end

end
