# see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#monitoring
defmodule Mongo.Monitor do
  @moduledoc false
  use GenServer
  use Bitwise
  require Logger
  alias Mongo.ServerDescription
  alias Mongo.Events.ServerHeartbeatStartedEvent
  alias Mongo.Events.{ServerHeartbeatStartedEvent, ServerHeartbeatFailedEvent,
                      ServerHeartbeatSucceededEvent}

  # this is not configurable because the specification says so
  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#minheartbeatfrequencyms
  @min_heartbeat_frequency_ms 500

  def start_link(args, gen_server_opts \\ []) do
    GenServer.start_link(__MODULE__, args, gen_server_opts)
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  def force_check(pid) do
    GenServer.call(pid, :check, :infinity)
  end

  ## GenServer callbacks

  @doc false
  def init([server_description, topology_pid, heartbeat_frequency_ms, connection_opts]) do
    opts = # monitors don't authenticate and use the "admin" database
      connection_opts
      |> Keyword.put(:database, "admin")
      |> Keyword.put(:skip_auth, true)
    {:ok, pid} = DBConnection.start_link(Mongo.Protocol, opts)
    :ok = GenServer.cast(self, :check)
    {:ok, %{connection_pid: pid, topology_pid: topology_pid,
            server_description: server_description,
            heartbeat_frequency_ms: heartbeat_frequency_ms}}
  end

  @doc false
  def handle_cast(:check, state) do
    check(state)
  end

  @doc false
  def handle_call(:check, _from, state) do
    check(state)
  end

  @doc false
  def handle_info(:timeout, state) do
    check(state)
  end

  ## Private functions

  defp check(state) do
    diff = :os.system_time(:milli_seconds) - state.server_description.last_update_time
    if diff < @min_heartbeat_frequency_ms do
      {:noreply, state, diff}
    else
      server_description = is_master(state.connection_pid, state.server_description)

      :ok = GenServer.call(state.topology_pid, {:server_description, server_description}, 30_000)
      {:noreply, %{state | server_description: server_description}, state.heartbeat_frequency_ms}
    end
  end

  defp call_is_master(conn_pid) do
    start_time = System.monotonic_time
    result = Mongo.direct_command(conn_pid, %{isMaster: 1})
    finish_time = System.monotonic_time
    rtt = System.convert_time_unit(finish_time - start_time, :native, :milli_seconds)
    finish_time = System.convert_time_unit(finish_time, :native, :milli_seconds)

    {result, finish_time, rtt}
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#network-error-when-calling-ismaster
  defp is_master(conn_pid, last_server_description) do
    :ok = GenEvent.notify(Mongo.Events, %ServerHeartbeatStartedEvent{
      connection_pid: conn_pid
    })

    {result, finish_time, rtt} = call_is_master(conn_pid)
    case result do
      {:ok, is_master_reply} ->
        notify_success(rtt, is_master_reply, conn_pid)
        ServerDescription.from_is_master(last_server_description, rtt, finish_time, is_master_reply)

      {:disconnect, error, _} ->
        if last_server_description.type in [:unknown, :possible_primary] do
          notify_error(rtt, error, conn_pid)
          ServerDescription.from_is_master_error(last_server_description, error)
        else
          {result, finish_time, rtt} = call_is_master(conn_pid)
          case result do
            {:ok, is_master_reply} ->
              notify_success(rtt, is_master_reply, conn_pid)
              ServerDescription.from_is_master(last_server_description, rtt, finish_time, is_master_reply)
            {:disconnect, error, _} ->
              notify_error(rtt, error, conn_pid)
              ServerDescription.from_is_master_error(last_server_description, error)
          end
        end
    end
  end

  defp notify_error(rtt, error, conn_pid) do
    :ok = GenEvent.notify(Mongo.Events, %ServerHeartbeatFailedEvent{
            duration: rtt,
             failure: error,
      connection_pid: conn_pid
    })
  end

  defp notify_success(rtt, reply, conn_pid) do
    :ok = GenEvent.notify(Mongo.Events, %ServerHeartbeatSucceededEvent{
      duration: rtt,
      reply: reply,
      connection_pid: conn_pid
    })
  end
end
