defmodule Mongo.Events.ServerDescriptionChangedEvent do
  defstruct [:address, :topology_pid, :previous_description, :new_description]
end

defmodule Mongo.Events.ServerOpeningEvent do
  defstruct [:address, :topology_pid]
end

defmodule Mongo.Events.ServerClosedEvent do
  defstruct [:address, :topology_pid]
end

defmodule Mongo.Events.TopologyDescriptionChangedEvent do
  defstruct [:topology_pid, :previous_description, :new_description]
end

defmodule Mongo.Events.TopologyOpeningEvent do
  defstruct [:topology_pid]
end

defmodule Mongo.Events.TopologyClosedEvent do
  defstruct [:topology_pid]
end

defmodule Mongo.Events.ServerHeartbeatStartedEvent do
  defstruct [:connection_pid]
end

defmodule Mongo.Events.ServerHeartbeatSucceededEvent do
   defstruct [:duration, :reply, :connection_pid]
end

defmodule Mongo.Events.ServerHeartbeatFailedEvent do
  defstruct [:duration, :failure, :connection_pid]
end
