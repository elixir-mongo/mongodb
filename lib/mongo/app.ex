defmodule Mongo.App do
  @moduledoc false

  def start(_type, _args) do
    children = [
      Mongo.IdServer,
      Mongo.PBKDF2Cache,
      Mongo.Session.Supervisor,
      %{
        id: Mongo.Events,
        start: {:gen_event, :start_link, local: Mongo.Events}
      }
    ]

    opts = [strategy: :one_for_one, name: Mongo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
