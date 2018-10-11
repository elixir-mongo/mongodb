defmodule Mongo.Events do
  @doc false

  def notify(event) do
    :gen_event.notify(__MODULE__, event)
  end

  # Published when server description changes, but does NOT include changes to
  # the RTT
  defmodule ServerDescriptionChangedEvent do
    @moduledoc false
    defstruct [:address, :topology_pid, :previous_description, :new_description]
  end

  # Published when server is initialized
  defmodule ServerOpeningEvent do
    @moduledoc false
    defstruct [:address, :topology_pid]
  end

  # Published when server is closed
  defmodule ServerClosedEvent do
    @moduledoc false
    defstruct [:address, :topology_pid]
  end

  # Published when topology description changes
  defmodule TopologyDescriptionChangedEvent do
    @moduledoc false
    defstruct [:topology_pid, :previous_description, :new_description]
  end

  # Published when topology is initialized
  defmodule TopologyOpeningEvent do
    @moduledoc false
    defstruct [:topology_pid]
  end

  # Published when topology is closed
  defmodule TopologyClosedEvent do
    @moduledoc false
    defstruct [:topology_pid]
  end

  # Fired when the server monitor’s ismaster command is started - immediately
  # before the ismaster command is serialized into raw BSON and written to the
  # socket.
  defmodule ServerHeartbeatStartedEvent do
    @moduledoc false
    defstruct [:connection_pid]
  end

  # Fired when the server monitor’s ismaster succeeds
  defmodule ServerHeartbeatSucceededEvent do
    @moduledoc false
    defstruct [:duration, :reply, :connection_pid]
  end

  # Fired when the server monitor’s ismaster fails, either with an “ok: 0” or
  # a socket exception.
  defmodule ServerHeartbeatFailedEvent do
    @moduledoc false
    defstruct [:duration, :failure, :connection_pid]
  end
end
