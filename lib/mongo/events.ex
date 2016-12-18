defmodule Mongo.Events do
  @doc false
  def notify(event) do
    GenEvent.notify(__MODULE__, event)
  end
end

defmodule Mongo.Events.ServerDescriptionChangedEvent do
  @moduledoc "Published when server description changes, but does NOT include changes to the RTT."
  defstruct [:address, :topology_pid, :previous_description, :new_description]
end

defmodule Mongo.Events.ServerOpeningEvent do
  @moduledoc "Published when server is initialized."
  defstruct [:address, :topology_pid]
end

defmodule Mongo.Events.ServerClosedEvent do
  @moduledoc "Published when server is closed."
  defstruct [:address, :topology_pid]
end

defmodule Mongo.Events.TopologyDescriptionChangedEvent do
  @moduledoc "Published when topology description changes."
  defstruct [:topology_pid, :previous_description, :new_description]
end

defmodule Mongo.Events.TopologyOpeningEvent do
  @moduledoc "Published when topology is initialized."
  defstruct [:topology_pid]
end

defmodule Mongo.Events.TopologyClosedEvent do
  @moduledoc "Published when topology is closed."
  defstruct [:topology_pid]
end

defmodule Mongo.Events.ServerHeartbeatStartedEvent do
  @moduledoc ~S"""
  Fired when the server monitor’s ismaster command is started - immediately before the ismaster
  command is serialized into raw BSON and written to the socket.
  """
  defstruct [:connection_pid]
end

defmodule Mongo.Events.ServerHeartbeatSucceededEvent do
  @moduledoc "Fired when the server monitor’s ismaster succeeds."
   defstruct [:duration, :reply, :connection_pid]
end

defmodule Mongo.Events.ServerHeartbeatFailedEvent do
  @moduledoc ~S"""
  Fired when the server monitor’s ismaster fails, either with an “ok: 0” or a socket exception.
  """
  defstruct [:duration, :failure, :connection_pid]
end
