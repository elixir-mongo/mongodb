defmodule Mongo.Pool.Adapter do
  @moduledoc """
  The driver supports multiple pooling strategies as long as they
  implement the following interface.

  An adapter should supervise processes of `Mongo.Connection` and
  provide the `transaction/2` function to execute an anonymous
  function with a connection from the pool.
  """
  use Behaviour

  @type name :: atom

  @doc """
  Starts any connection pooling and supervision.
  """
  defcallback start_link(name, Keyword.t) :: GenServer.on_start

  @doc """
  Runs the function with a checked out connection from the pool,
  after the function returns the pool should reclaim the connection.
  """
  defcallback transaction(name, (pid -> return)) :: return
         when return: var
end
