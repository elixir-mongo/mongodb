defmodule Mongo.Events do
  @doc false

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def notify(event) do
    GenServer.cast(__MODULE__, { :notify, event })
  end

  def wait_for_event(module, timeout) do
    GenServer.call(__MODULE__, { :add_client, self() })
    try do
      receive do
        { :notification_received, event = %{ __struct__: ^module }} ->
          event
      after
        timeout ->
          :timeout
      end
    after
      GenServer.call(__MODULE__, { :remove_client, self() })
    end
  end

  # server impl
  def init(_) do
    Process.flag(:trap_exit, true)
    { :ok, %{ clients: [] } }
  end

  def handle_call({ :add_client, pid}, _, state = %{ clients: clients}) do
    Process.link(pid)
    { :reply, :ok, %{ state | clients: [ pid | clients ] }}
  end

  def handle_call({ :remove_client, pid}, _, state = %{ clients: clients}) do
    Process.unlink(pid)
    { :reply, :ok, %{ state | clients: remove_client(pid, clients) }}
  end

  def handle_cast({:notify, event}, state = %{ clients: clients }) do
    clients
    |> Enum.map(&send(&1, { :notification_received, event }))
    { :noreply, state }
  end

  def handle_info({:EXIT, pid, _reason}, state = %{ clients: clients }) do
    {:noreply, %{ state | clients: remove_client(pid, clients) }}
  end

  defp remove_client(pid, clients) do
    clients
    |> Enum.reject(fn client -> client == pid end)
  end

  defmodule ServerDescriptionChangedEvent do
    @moduledoc "Published when server description changes, but does NOT include changes to the RTT."
    defstruct [:address, :topology_pid, :previous_description, :new_description]
  end

  defmodule ServerOpeningEvent do
    @moduledoc "Published when server is initialized."
    defstruct [:address, :topology_pid]
  end

  defmodule ServerClosedEvent do
    @moduledoc "Published when server is closed."
    defstruct [:address, :topology_pid]
  end

  defmodule TopologyDescriptionChangedEvent do
    @moduledoc "Published when topology description changes."
    defstruct [:topology_pid, :previous_description, :new_description]
  end

  defmodule TopologyOpeningEvent do
    @moduledoc "Published when topology is initialized."
    defstruct [:topology_pid]
  end

  defmodule TopologyClosedEvent do
    @moduledoc "Published when topology is closed."
    defstruct [:topology_pid]
  end

  defmodule ServerHeartbeatStartedEvent do
    @moduledoc ~S"""
    Fired when the server monitor’s ismaster command is started - immediately before the ismaster
    command is serialized into raw BSON and written to the socket.
    """
    defstruct [:connection_pid]
  end

  defmodule ServerHeartbeatSucceededEvent do
    @moduledoc "Fired when the server monitor’s ismaster succeeds."
    defstruct [:duration, :reply, :connection_pid]
  end

  defmodule ServerHeartbeatFailedEvent do
    @moduledoc ~S"""
    Fired when the server monitor’s ismaster fails, either with an “ok: 0” or a socket exception.
    """
    defstruct [:duration, :failure, :connection_pid]
  end
end
