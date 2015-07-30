defmodule Mongo.Pool do
  @moduledoc """
  Defines a pool of MongoDB connections.

  A pool can be defined as:

      defmodule MyPool do
        use Mongo.Pool,
          adapter: Mongo.Pool.Poolboy,
          hostname: "localhost"
       end

  Options will be passed to the pool adapter and to `Mongo.Connection`.

  ## Logging

  The pool may define a `log/5` function, that will be called by the
  driver on every call to the database.

  Please refer to the callback's documentation for more information.
  """

  use Behaviour
  alias Mongo.Pool.Monitor

  @type t :: module

  @doc false
  defmacro __using__(opts) do
    adapter = Keyword.fetch!(opts, :adapter)

    quote do
      # TODO: Customizable timeout
      @timeout   5_000
      @behaviour unquote(__MODULE__)
      @adapter   unquote(adapter)
      @name      __MODULE__
      @sup       __MODULE__.Sup
      @monitor   __MODULE__.Monitor
      @ets       __MODULE__.ETS

      @doc false
      def start_link(opts) do
        import Supervisor.Spec, warn: false

        children = [
          worker(Monitor, [@monitor, @ets, opts]),
          worker(@adapter, [@name, opts]),
        ]

        opts = [strategy: :one_for_all, name: @sup]
        Supervisor.start_link(children, opts)
      end

      @doc false
      def stop do
        Process.whereis(__MODULE__)
        |> Process.exit(:shutdown)
      end

      @doc false
      def run(fun) do
        @adapter.run(@name, fun)
      end

      @doc false
      def version do
        Monitor.version(@monitor, @ets, @timeout)
      end

      @doc false
      def log(return, queue_time, query_time, _fun, _args) do
        return
      end

      defoverridable [log: 5]
    end
  end

  @type time :: integer
  @type operation ::
    :run_command | :insert_one | :insert_many | :delete_one | :delete_many |
    :replace_one | :update_one | :update_many | :find_cursor | :find_batch |
    :kill_cursors

  @doc """
  Executes given function checking out a connection from pool, and ensuring it
  will be properely checked in back once finished.
  """
  defcallback run((pid -> return)) :: {queue_time :: time, return} when return: var

  @doc """
  Returns the version of the MongoDB wire protocol used for the pool's connections
  """
  defcallback version() :: non_neg_integer

  @doc """
  Called every time when the driver has a logging information to be printed.

  The first argument result can be of form: `:ok`, `{:ok, _}` or `{:error, _}`.
  The second element of the tuples should be considered private, and not used.

  ## Operations

  The fourth argument determines the operation, these can be (listed with the
  arguments passed as the fifth argument to the log function):

  Operation       | Arguments
  :-------------- | :-------------------------------------------
  `:run_command`  | `[query, options]`
  `:insert_one`   | `[collection, document, options]`
  `:insert_many`  | `[collection, documents, options]`
  `:delete_one`   | `[collection, filter, options]`
  `:delete_many`  | `[collection, filter, options]`
  `:replace_one`  | `[collection, filter, replacement, options]`
  `:update_one`   | `[collection, filter, update, options]`
  `:update_many`  | `[collection, filter, update, options]`
  `:find`         | `[collection, query, projection, options]`
  `:find_rest`    | `[collection, cursor, options]`
  `:kill_cursors` | `[cursors, options]`
  """
  defcallback log(return, queue_time, query_time, operation, args :: list) ::
    return when return: var, queue_time: time, query_time: time


  @doc """
  Invokes given pool's `run/1` gathering information necessary for the pools
  `log/5` function.

  The `opts` argument is appended to the `args` list passed to the pool's
  log function.

  ## Options

    * `:log` - if `false` the `log/5` function won't be invoked (default: `true`)
  """
  def run_with_log(pool, log, args, opts, fun) do
    {log?, opts} = Keyword.pop(opts, :log, true)

    if log? do
      {queue_time, {query_time, value}} =
        pool.run(&:timer.tc(fun, [&1]))

      pool.log(value, queue_time, query_time, log, args ++ [opts])
      value
    else
      {_queue_time, value} = pool.run(fun)
      value
    end
  end
end
