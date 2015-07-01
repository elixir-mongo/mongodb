import Record, only: [defrecordp: 2]
alias Mongo.Connection
alias Mongo.ReadResult

defmodule Mongo.Cursor do
  defstruct [:pool, :coll, :query, :select, :opts]

  defimpl Enumerable do
    defrecordp :state, [:pool, :cursor, :buffer, :limit]

    def reduce(%{pool: pool, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      limit = opts[:limit]

      start_fun = start_fun(pool, coll, query, select, limit, opts)
      next_fun  = next_fun(coll, opts)
      after_fun = after_fun()

      Stream.resource(start_fun, next_fun, after_fun).(acc, reduce_fun)
    end

    defp start_fun(pool, coll, query, projector, limit, opts) do
      opts = batch_size(limit, opts)

      fn ->
        pool.transaction(fn pid ->
          case Connection.find(pid, coll, query, projector, opts) do
            {:ok, %ReadResult{cursor_id: cursor, docs: docs, num: num}} ->
              state(pool: pool, cursor: cursor, buffer: docs, limit: new_limit(limit, num))
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

        state(buffer: [], limit: 0) = state ->
          {:halt, state}

        # Work around num_return=1 closing cursor immediately
        # state(buffer: [doc, _], limit: 3) = state ->
        #   {[doc], state(state, buffer: [], limit: 0)}

        state(buffer: [], limit: limit, pool: pool, cursor: cursor) = state ->
          opts = batch_size(limit, opts)

          pool.transaction(fn pid ->
            case Connection.get_more(pid, coll, cursor, opts) do
              {:ok, %ReadResult{cursor_id: cursor, docs: []}} ->
                {:halt, state(state, cursor: cursor)}
              {:ok, %ReadResult{cursor_id: cursor, docs: docs, num: num}} ->
                {docs, state(state, cursor: cursor, limit: new_limit(limit, num))}
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
            Connection.kill_cursors(pid, [cursor])
          end)
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
  defstruct [:pool, :coll, :query, :select, :opts]

  defimpl Enumerable do
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
          opts = Keyword.put(opts, :batch_size, -1)

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
            Connection.kill_cursors(pid, [cursor])
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
    defrecordp :state, [:pool, :cursor, :buffer]

    def reduce(%{pool: pool, coll: coll, query: query, select: select, opts: opts},
               acc, reduce_fun) do
      opts      = Keyword.put(opts, :batch_size, -1)
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
