defmodule Mongo.Pool.Adapter do
  use Behaviour

  @type name :: atom

  defcallback start_link(name, Keyword.t) :: GenServer.on_start
  defcallback transaction(name, (pid -> any)) :: any
end
