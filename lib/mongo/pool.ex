defmodule Mongo.Pool do
  use Behaviour

  defmacro __using__(opts) do
    adapter = Keyword.fetch!(opts, :adapter)
    name    = Keyword.fetch!(opts, :name)

    quote do
      @behaviour unquote(__MODULE__)
      @adapter   unquote(adapter)
      @name      unquote(name).Adapter

      def start_link(opts) do
        import Supervisor.Spec, warn: false

        children = [
          worker(Mongo.Pool.Monitor, [@name, opts]),
          worker(@adapter, [@name, opts])
        ]

        opts = [strategy: :one_for_all, name: __MODULE__]
        Supervisor.start_link(children, opts)
      end

      def stop do
        Process.whereis(__MODULE__)
        |> Process.exit(:shutdown)
      end

      def transaction(fun) do
        @adapter.transaction(@name, fun)
      end

      @wait_sleep 10
      @wait_usec 5_000_000

      def version do
        now = now()
        waiting_version(now, now + @wait_usec)
      end

      # We use the waiting_version hack to work around the race between the
      # first clients and Mongo.Pool.Monitor setting the version in the ets
      # table
      defp waiting_version(now, stop_at) when now < stop_at do
        case :ets.lookup(@name, :wire_version) do
          [] ->
            :timer.sleep(@wait_sleep)
            waiting_version(now(), stop_at)
          [{:wire_version, version}] ->
            version
        end
      end

      defp waiting_version(_now, _stop_at) do
        raise "version request from #{inspect @name} timed out"
      end

      defp now do
        {msec, sec, usec} = :os.timestamp
        (msec*1_000_000+sec)*1_000_000+usec
      end
    end
  end

  defcallback transaction((pid -> any)) :: any
  defcallback version() :: non_neg_integer
end
