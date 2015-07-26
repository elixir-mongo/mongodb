defmodule Mongo.Pool do
  use Behaviour
  alias Mongo.Pool.Monitor

  defmacro __using__(opts) do
    adapter = Keyword.fetch!(opts, :adapter)

    quote do
      # TODO: Customize timeout
      @timeout  5_000
      @behaviour unquote(__MODULE__)
      @adapter   unquote(adapter)
      @name      __MODULE__
      @sup       __MODULE__.Sup
      @monitor   __MODULE__.Monitor
      @ets       __MODULE__.ETS

      def start_link(opts) do
        import Supervisor.Spec, warn: false

        children = [
          worker(Monitor, [@monitor, @ets, opts]),
          worker(@adapter, [@name, opts])
        ]

        opts = [strategy: :one_for_all, name: @sup]
        Supervisor.start_link(children, opts)
      end

      def stop do
        Process.whereis(__MODULE__)
        |> Process.exit(:shutdown)
      end

      def transaction(fun) do
        @adapter.transaction(@name, fun)
      end

      def version do
        case :ets.lookup(@ets, :wire_version) do
          [] ->
            Monitor.version(@monitor, @timeout)
          [{:wire_version, version}] ->
            version
        end
      end
    end
  end

  defcallback transaction((pid -> any)) :: any
  defcallback version() :: non_neg_integer
end
