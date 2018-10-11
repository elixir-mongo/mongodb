defmodule Mongo.App do
  @moduledoc false

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      worker(Mongo.IdServer, []),
      worker(Mongo.PBKDF2Cache, []),
      worker(:gen_event, [local: Mongo.Events])
    ]

    opts = [strategy: :one_for_one, name: Mongo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
