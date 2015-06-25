defmodule Mongo.Cursor do
  defstruct [:pool, :coll, :query, :select, :opts]

  defimpl Enumerable do
    import Record, only: [defrecordp: 2]
    alias Mongo.Connection
    alias Mongo.ReadResult

    defrecordp :state, [:pool, :cursor, :buffer]

    def reduce(%{pool: pool, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      start_fun = start_fun(pool, coll, query, select, opts)
      next_fun  = next_fun(coll, opts)
      after_fun = after_fun()

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    defp start_fun(pool, coll, query, projector, opts) do
      fn ->
        pool.transaction(fn pid ->
          case Connection.find(pid, coll, query, projector, opts) do
            {:ok, %ReadResult{cursor_id: cursor, docs: docs}} ->
              state(pool: pool, cursor: cursor, buffer: docs)
            {:error, error} ->
              raise error
          end
        end)
      end
    end

    defp next_fun(coll, opts) do
      fn
        state(buffer: [], cursor: 0) = state ->
          {:halt, state}

        state(buffer: [], pool: pool, cursor: cursor) = state ->
          pool.transaction(fn pid ->
            case Connection.get_more(pid, coll, cursor, opts) do
              {:ok, %ReadResult{cursor_id: cursor, docs: []}} ->
                {:halt, state(state, cursor: cursor)}
              {:ok, %ReadResult{cursor_id: cursor, docs: docs}} ->
                {docs, state(state, cursor: cursor)}
              {:error, error} ->
                raise error
            end
          end)

        state(buffer: buffer) = state ->
          {buffer, state(state, buffer: [])}
      end
    end

    defp after_fun do
      fn
        state(cursor: 0) ->
          :ok
        state(cursor: cursor, pool: pool) ->
          pool.transaction(fn pid ->
            Mongo.Connection.kill_cursors(pid, [cursor])
          end)
      end
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _term) do
      {:error, __MODULE__}
    end
  end
end

defmodule Mongo.AggregationCursor do
  defstruct [:pool, :coll, :query, :select, :opts]

  defimpl Enumerable do
    import Record, only: [defrecordp: 2]
    alias Mongo.Connection
    alias Mongo.ReadResult

    defrecordp :state, [:pool, :cursor, :coll, :buffer]

    def reduce(%{pool: pool, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      start_fun = start_fun(pool, coll, query, select, opts)
      next_fun  = next_fun(opts)
      after_fun = after_fun()

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    defp start_fun(pool, coll, query, projector, opts) do
      fn ->
        pool.transaction(fn pid ->
          opts = Keyword.put(opts, :num_return, -1)

          case Connection.find(pid, coll, query, projector, opts) do
            {:ok, %ReadResult{cursor_id: 0, docs: [%{"ok" => 1.0, "cursor" => %{"id" => cursor, "ns" => coll, "firstBatch" => docs}}]}} ->
              state(pool: pool, cursor: cursor, coll: coll, buffer: docs)
            {:error, error} ->
              raise error
          end
        end)
      end
    end

    defp next_fun(opts) do
      fn
        state(buffer: [], cursor: 0) = state ->
          {:halt, state}

        state(buffer: [], pool: pool, cursor: cursor, coll: coll) = state ->
          pool.transaction(fn pid ->
            case Connection.get_more(pid, {:override, coll}, cursor, opts) do
              {:ok, %ReadResult{cursor_id: cursor, docs: []}} ->
                {:halt, state(state, cursor: cursor)}
              {:ok, %ReadResult{cursor_id: cursor, docs: docs}} ->
                {docs, state(state, cursor: cursor)}
              {:error, error} ->
                raise error
            end
          end)

        state(buffer: buffer) = state ->
          {buffer, state(state, buffer: [])}
      end
    end

    defp after_fun do
      fn
        state(cursor: 0) ->
          :ok
        state(cursor: cursor, pool: pool) ->
          pool.transaction(fn pid ->
            Mongo.Connection.kill_cursors(pid, [cursor])
          end)
      end
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
  defstruct [:pool, :coll, :query, :select, :opts]

  defimpl Enumerable do
    import Record, only: [defrecordp: 2]
    alias Mongo.Connection
    alias Mongo.ReadResult

    defrecordp :state, [:pool, :cursor, :buffer]

    def reduce(%{pool: pool, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      opts      = Keyword.put(opts, :num_return, -1)
      start_fun = start_fun(pool, coll, query, select, opts)
      next_fun  = next_fun()
      after_fun = after_fun()

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    defp start_fun(pool, coll, query, projector, opts) do
      fn ->
        pool.transaction(fn pid ->
          case Connection.find(pid, coll, query, projector, opts) do
            {:ok, %ReadResult{cursor_id: 0, docs: [%{"ok" => 1.0, "result" => docs}]}} ->
              docs
            {:error, error} ->
              raise error
          end
        end)
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
