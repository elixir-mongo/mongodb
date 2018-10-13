defmodule Mongo.GridFs.UploadStream do
  @moduledoc false

  use Timex
  import Record, only: [defrecordp: 2]

  alias Mongo.GridFs.UploadStream
  alias Mongo.GridFs.Bucket

  defstruct bucket: nil, id: nil, filename: nil, meta: %{}

  def new(bucket, filename, opts \\ []) do
    %UploadStream{bucket: bucket, filename: filename, id: Mongo.IdServer.new()}
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

    def make_fun( %UploadStream{bucket: %{chunk_size: chunk_size}} = stream) do
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
          flush_buffer(stream, s)
          stream

        # steam halts, write the rest
        state() = s, :halt ->
          flush_buffer(stream, s)
          :ok
      end
    end

    ##
    # flushes the buffer and creates the files document
    #
    defp flush_buffer(%UploadStream{ bucket: %Bucket{ conn: conn, chunk_size: chunk_size} = bucket,
                                     filename: filename,
                                     id: file_id},
                    state( buffer: buffer, number: chunk_number) ) do

      collection = Bucket.chunks_collection_name(bucket)
      length = chunk_number * chunk_size + byte_size(buffer)
      insert_one_chunk_document(conn, collection, file_id, buffer, chunk_number)

      collection = Bucket.files_collection_name(bucket)
      insert_one_file_document(conn, collection, file_id, length, chunk_size, filename) ## todo filename

    end

    ##
    # checks if the buffer is smaller than the chunk-size
    # in this case we do nothing
    #
    defp write_buffer(%UploadStream{bucket: %Bucket{chunk_size: chunk_size}},
                    state( buffer: buffer) = s ) when byte_size( buffer ) < chunk_size  do
      s
    end

    ##
    # otherwise we
    # write the data to the chunk collections and call the function again with the rest of the buffer
    # for the case that the buffer size is still greater than the chunk size
    #
    defp write_buffer(%UploadStream{bucket: %Bucket{ conn: conn, chunk_size: chunk_size} = bucket, id: file_id} = stream,
                  state(buffer: buffer, number: chunk_number)) do

      collection = Bucket.chunks_collection_name(bucket)
      fun = fn ( <<data::bytes-size(chunk_size), rest :: binary>> ) ->
        next = insert_one_chunk_document(conn, collection, file_id, data, chunk_number)
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
    defp insert_one_chunk_document(_conn, _collection, _file_id, data, chunk_number) when byte_size(data) == 0 do
      chunk_number
    end

    defp insert_one_chunk_document(conn, collection, file_id, data, chunk_number) do
      {:ok, _} = Mongo.insert_one( conn, collection, %{files_id: file_id, n: chunk_number, data: data})
      chunk_number + 1
    end

    ##
    # inserts one file document
    #
    defp insert_one_file_document( conn, collection, file_id, length, chunk_size, filename ) do
      now = Timex.now
      {:ok, _} = Mongo.insert_one( conn, collection, %{_id: file_id, length: length, filename: filename, chunkSize: chunk_size, uploadDate: now})  ## todo collection!
    end

  end

end
