import Record, only: [defrecordp: 2]

# TODO: Check options to Mongo function

# TODO: Handle error responses from Mongo.raw_find, example:
# {:ok, %{cursor_id: 0, docs: [%{"code" => 16436, "errmsg" => "exception: Unrecognized pipeline stage name: '$projection'", "ok" => 0.0}], from: 0, num: 1}}

defmodule Mongo.Cursor do
  @moduledoc false

  @type t :: %__MODULE__{
    conn: Mongo.conn,
    coll: Mongo.collection,
    query: BSON.document,
    select: BSON.document | nil,
    opts: Keyword.t
  }
  defstruct [:conn, :coll, :query, :select, :opts]

  defimpl Enumerable do
    defrecordp :state, [:conn, :cursor, :buffer, :limit]

    def reduce(%{conn: conn, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      limit      = opts[:limit]
      opts       = Keyword.drop(opts, [:limit])
      next_opts  = Keyword.drop(opts, [:limit, :skip])

      start_fun = start_fun(conn, coll, query, select, limit, opts)
      next_fun  = next_fun(coll, next_opts)
      after_fun = after_fun(next_opts)

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    # we cannot determinstically slice, so tell Enumerable to
    # fall back on brute force
    def slice(_cursor) do
      { :error, __MODULE__ }
    end

    defp start_fun(conn, coll, query, projector, limit, opts) do
      opts = batch_size(limit, opts)

      fn ->
        result = Mongo.raw_find(conn, coll, query, projector, opts)

        case result do
          {:ok, %{cursor_id: cursor, docs: docs, num: num}} ->
            state(conn: conn, cursor: cursor, buffer: docs, limit: new_limit(limit, num))
          {:error, error} ->
            raise error
        end
      end
    end

    defp next_fun(coll, opts) do
      fn
        state(buffer: [], cursor: 0) = state ->
          {:halt, state}

        state(buffer: [], limit: 0) = state ->
          {:halt, state}

        # Work around num_return=1 closing cursor immediately
        # state(buffer: [doc, _], limit: 3) = state ->
        #   {[doc], state(state, buffer: [], limit: 0)}

        state(buffer: [], limit: limit, conn: conn, cursor: cursor) = state ->
          opts = batch_size(limit, opts)

          case Mongo.get_more(conn, coll, cursor, opts) do
            {:ok, %{cursor_id: cursor, docs: []}} ->
              {:halt, state(state, cursor: cursor)}
            {:ok, %{cursor_id: cursor, docs: docs, num: num}} ->
              {docs, state(state, cursor: cursor, limit: new_limit(limit, num))}
            {:error, error} ->
              raise error
          end

        state(buffer: buffer) = state ->
          {buffer, state(state, buffer: [])}
      end
    end

    defp after_fun(opts) do
      fn
        state(cursor: 0) ->
          :ok
        state(cursor: cursor, conn: conn) ->
          Mongo.kill_cursors(conn, [cursor], opts)
      end
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _term) do
      {:error, __MODULE__}
    end

    defp batch_size(limit, opts) do
      batch_size = Enum.reject([opts[:batch_size], limit, 1000], &is_nil/1) |> Enum.min
      opts = Keyword.drop(opts, ~w(batch_size limit)a)
      [batch_size: batch_size] ++ opts
    end

    defp new_limit(nil, _),
      do: nil
    defp new_limit(limit, num),
      do: limit-num
  end
end

defmodule Mongo.AggregationCursor do
  @moduledoc false

  @type t :: %__MODULE__{
    conn: Mongo.conn,
    coll: Mongo.collection,
    query: BSON.document,
    select: BSON.document | nil,
    opts: Keyword.t
  }
  defstruct [:conn, :coll, :query, :select, :opts]

  defimpl Enumerable do
    defrecordp :state, [:conn, :cursor, :coll, :buffer]

    def reduce(%{conn: conn, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      start_fun = start_fun(conn, coll, query, select, opts)
      next_fun  = next_fun(opts)
      after_fun = after_fun(opts)

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    # we cannot determinstically slice, so tell Enumerable to
    # fall back on brute force
    def slice(_cursor) do
      { :error, __MODULE__ }
    end

    defp start_fun(conn, coll, query, projector, opts) do
      opts = Keyword.put(opts, :batch_size, -1)

      fn ->
        case Mongo.raw_find(conn, coll, query, projector, opts) do
          {:ok, %{cursor_id: 0, docs: [%{"ok" => ok, "cursor" => %{"id" => cursor, "ns" => coll, "firstBatch" => docs}}]}} when ok == 1->
            state(conn: conn, cursor: cursor, coll: only_coll(coll), buffer: docs)
          {:error, error} ->
            raise error
        end
      end
    end

    defp next_fun(opts) do
      fn
        state(buffer: [], cursor: 0) = state ->
          {:halt, state}

        state(buffer: [], conn: conn, cursor: cursor, coll: coll) = state ->
          case Mongo.get_more(conn, coll, cursor, opts) do
            {:ok, %{cursor_id: cursor, docs: []}} ->
              {:halt, state(state, cursor: cursor)}
            {:ok, %{cursor_id: cursor, docs: docs}} ->
              {docs, state(state, cursor: cursor)}
            {:error, error} ->
              raise error
          end

        state(buffer: buffer) = state ->
          {buffer, state(state, buffer: [])}
      end
    end

    defp after_fun(opts) do
      fn
        state(cursor: 0) ->
          :ok
        state(cursor: cursor, conn: conn) ->
          Mongo.kill_cursors(conn, [cursor], opts)
      end
    end

    defp only_coll(coll) do
      [_db, coll] = String.split(coll, ".", parts: 2)
      coll
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _term) do
      {:error, __MODULE__}
    end
  end
end

defmodule Mongo.SinglyCursor do
  @moduledoc false

  @type t :: %__MODULE__{
    conn: Mongo.conn,
    coll: Mongo.collection,
    query: BSON.document,
    select: BSON.document | nil,
    opts: Keyword.t
  }
  defstruct [:conn, :coll, :query, :select, :opts]

  defimpl Enumerable do
    def reduce(%{conn: conn, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      opts      = Keyword.put(opts, :batch_size, -1)
      start_fun = start_fun(conn, coll, query, select, opts)
      next_fun  = next_fun()
      after_fun = after_fun()

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    def slice(_cursor) do
      { :error, __MODULE__ }
    end

    defp start_fun(conn, coll, query, projector, opts) do
      fn ->
        case Mongo.raw_find(conn, coll, query, projector, opts) do
          {:ok, %{cursor_id: 0, docs: [%{"ok" => ok, "result" => docs}]}} when ok == 1 ->
            docs
          {:error, error} ->
            raise error
        end
      end
    end

    defp next_fun do
      fn
        [] -> {:halt, :ok}
        docs -> {docs, []}
      end
    end

    defp after_fun do
      fn _ -> :ok end
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _term) do
      {:error, __MODULE__}
    end
  end
end
