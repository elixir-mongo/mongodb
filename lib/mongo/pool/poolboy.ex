defmodule Mongo.Pool.Poolboy do
  @moduledoc """
  poolboy implementation of `Mongo.Pool.Adapter`.
  """

  @behaviour Mongo.Pool.Adapter
  @poolboy_opts ~w(max_overflow)

  @doc """
  Starts the poolboy pool.

  ## Options

    :pool_size - The number of connections to keep in the pool (default: 10)
    :max_overflow - The maximum overflow of connections (default: 0) (see poolboy docs)
  """
  def start_link(name, opts) do
    {size, opts} = Keyword.pop(opts, :pool_size, 10)
    {pool_opts, worker_opts} = Keyword.split(opts, @poolboy_opts)

    # TODO: Rename size => pool_size

    pool_opts = pool_opts
      |> Keyword.put(:name, {:local, name})
      |> Keyword.put(:worker_module, Mongo.Connection)
      |> Keyword.put(:size, size)
      |> Keyword.put_new(:max_overflow, 0)

    :poolboy.start_link(pool_opts, worker_opts)
  end

  def run(pool, fun) do
    {queue_time, pid} = :timer.tc(:poolboy, :checkout, [pool])
    ret =
      try do
        fun.(pid)
      after
        :ok = :poolboy.checkin(pool, pid)
      end

    {queue_time, ret}
  end
end
