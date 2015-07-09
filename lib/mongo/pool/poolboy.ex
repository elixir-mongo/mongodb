defmodule Mongo.Pool.Poolboy do
  @behaviour Mongo.Pool.Adapter
  @poolboy_opts ~w(size max_overflow)

  def start_link(name, opts) do
    {pool_opts, worker_opts} = Keyword.split(opts, @poolboy_opts)
    pool_opts = [
      name: {:local, name},
      worker_module: Mongo.Connection
    ] ++ pool_opts

    :poolboy.start_link(pool_opts, worker_opts)
  end

  def transaction(pool, fun) do
    :poolboy.transaction(pool, fun)
  end
end
