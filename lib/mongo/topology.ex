defmodule Mongo.Topology do
  @moduledoc false

  use GenServer
  alias Mongo.Events.{ServerDescriptionChangedEvent, ServerOpeningEvent, ServerClosedEvent,
                      TopologyDescriptionChangedEvent, TopologyOpeningEvent, TopologyClosedEvent}
  alias Mongo.TopologyDescription
  alias Mongo.ServerDescription
  alias Mongo.Monitor

  # https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#heartbeatfrequencyms-defaults-to-10-seconds-or-60-seconds
  @heartbeat_frequency_ms 10_000

  @spec start_link(Keyword.t, Keyword.t) ::
          {:ok, pid} |
          {:error, reason :: atom}
  def start_link(opts, gen_server_opts \\ []) do
    gen_server_opts =
      opts
      |> Keyword.take([:debug, :name, :timeout, :spawn_opt])
      |> Keyword.merge(gen_server_opts)
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  def connection_for_address(pid, address) do
    GenServer.call(pid, {:connection, address})
  end

  def wait_for_connection(pid, timeout, start_time) do
    timeout = timeout - System.convert_time_unit(System.monotonic_time - start_time, :native, :millisecond)

    wait_for_connection(pid, timeout)
  end

  defp wait_for_connection(pid, timeout) when timeout >= 0 do
    try do
      case GenServer.call(pid, :wait_for_connection, timeout) do
        {:new_connection, server} ->
          {:ok, [server]}
        {:connected, servers} ->
          {:ok, servers}
      end
    catch
      :exit, {:timeout, _} ->
        {:error, :selection_timeout}
    end
  end
  defp wait_for_connection(pid, _timeout) do
    wait_for_connection(pid, 0)
  end

  def topology(pid) do
    GenServer.call(pid, :topology)
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  ## GenServer Callbacks

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#configuration
  @doc false
  def init(opts) do
    seeds = Keyword.get(opts, :seeds, [seed(opts)])
    type = Keyword.get(opts, :type, :unknown)
    set_name = Keyword.get(opts, :set_name, nil)
    local_threshold_ms = Keyword.get(opts, :local_threshold_ms, 15)

    :ok = Mongo.Events.notify(%TopologyOpeningEvent{
      topology_pid: self()
    })

    cond do
      type == :single and length(seeds) > 1 ->
        {:stop, :single_topology_multiple_hosts}
      set_name != nil and not(type in [:unknown, :replica_set_no_primary, :single]) ->
        {:stop, :set_name_bad_topology}
      true ->
        servers = servers_from_seeds(seeds)
        state =
          %{
            topology: TopologyDescription.defaults(%{
              type: type,
              set_name: set_name,
              servers: servers,
              local_threshold_ms: local_threshold_ms
            }),
            seeds: seeds,
            opts: opts,
            monitors: %{},
            connection_pools: %{},
            waiting_pids: []
          }
          |> reconcile_servers
        {:ok, state}
    end
  end

  defp seed(opts) do
    case Mongo.Protocol.Utils.hostname_port(opts) do
      {{:local, socket}, 0} -> socket
      {hostname, port} -> "#{hostname}:#{port}"
    end
  end

  def terminate(_reason, state) do
    Enum.each(state.connection_pools, fn {_address, pid} -> GenServer.stop(pid) end)
    Enum.each(state.monitors, fn {_address, pid} -> GenServer.stop(pid) end)
    :ok = Mongo.Events.notify(%TopologyClosedEvent{
      topology_pid: self()
    })
  end

  def handle_call(:topology, _from, state) do
    {:reply, state.topology, state}
  end

  def handle_call({:connection, address}, _from, state) do
    {:reply, Map.fetch(state.connection_pools, address), state}
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#updating-the-topologydescription
  def handle_call({:server_description, server_description}, _from, state) do
    new_state = handle_server_description(state, server_description)
    if state.topology != new_state.topology do
      :ok = Mongo.Events.notify(%TopologyDescriptionChangedEvent{
        topology_pid: self(),
        previous_description: state.topology,
        new_description: new_state.topology
      })
    end
    {:reply, :ok, new_state}
  end

  def handle_call(:wait_for_connection, _from, %{connection_pools: pools} = state) when map_size(pools) > 0 do
    servers = Enum.map(pools, fn {key, _value} -> key end)
    {:reply, {:connected, servers}, state}
  end
  def handle_call(:wait_for_connection, from, %{waiting_pids: waiting} = state) do
    {:noreply, %{state | waiting_pids: [from | waiting]}}
  end

  def handle_cast(:reconcile, state) do
    new_state = reconcile_servers(state)
    {:noreply, new_state}
  end
  def handle_cast({:disconnect, :monitor, host}, state) do
    new_state = remove_address(host, state)
    maybe_reinit(new_state)
    {:noreply, new_state}
  end
  def handle_cast({:disconnect, :client, _host}, state) do
    {:noreply, state}
  end

  def handle_cast({:connected, monitor_pid}, state) do
    monitor = Enum.find(state.monitors, fn {_key, value} -> value == monitor_pid end)
    new_state = case monitor do
      nil ->
        state
      {host, ^monitor_pid} ->
        arbiters = fetch_arbiters(state)
        if host in arbiters do
          state
        else
          conn_opts =
            state.opts
            |> Keyword.put(:connection_type, :client)
            |> Keyword.put(:topology_pid, self())
            |> connect_opts_from_address(host)

          {:ok, pool} = DBConnection.start_link(Mongo.Protocol, conn_opts)
          connection_pools = Map.put(state.connection_pools, host, pool)
          Enum.each(state.waiting_pids, fn from ->
            GenServer.reply(from, {:new_connection, host})
          end)
          %{ state | connection_pools: connection_pools, waiting_pids: [] }
        end
    end
    {:noreply, new_state}
  end

  def handle_cast({:force_check, server_address}, state) do
    case Map.fetch(state.monitors, server_address) do
      {:ok, monitor_pid} ->
        :ok = Monitor.force_check(monitor_pid)
        {:noreply, state}

      :error ->
        # ignore force checks on monitors that don't exist
        {:noreply, state}
    end
  end

  defp handle_server_description(state, server_description) do
    state
    |> get_and_update_in([:topology],
                         &TopologyDescription.update(&1, server_description, length(state.seeds)))
    |> process_events
    |> reconcile_servers
  end

  defp process_events({events, state}) do
    Enum.each(events, fn
      {:force_check, _} = message ->
        :ok = GenServer.cast(self(), message)
      {previous, next} ->
        if previous != next do
          :ok = Mongo.Events.notify(%ServerDescriptionChangedEvent{
            address: next.address,
            topology_pid: self(),
            previous_description: previous,
            new_description: next
          })
        end
      _ ->
        :ok
    end)
    state
  end

  defp reconcile_servers(state) do
    arbiters = fetch_arbiters(state)
    old_addrs = Map.keys(state.monitors)
    # remove arbiters from connection pool as descriptions are recieved
    new_addrs = Map.keys(state.topology.servers) -- arbiters

    added = new_addrs -- old_addrs
    removed = old_addrs -- new_addrs

    state = Enum.reduce(added, state, fn (address, state) ->
      server_description = state.topology.servers[address]
      connopts = connect_opts_from_address(state.opts, address)
      args = [
        server_description,
        self(),
        @heartbeat_frequency_ms,
        Keyword.put(connopts, :pool, DBConnection.ConnectionPool)
      ]

      :ok = Mongo.Events.notify(%ServerOpeningEvent{address: address, topology_pid: self()})

      {:ok, pid} = Monitor.start_link(args)
      %{ state | monitors: Map.put(state.monitors, address, pid) }
    end)

    Enum.reduce(removed, state, &remove_address/2)
  end

  defp maybe_reinit(%{monitors: monitors} = state) when map_size(monitors) > 0,
    do: state
  defp maybe_reinit(state) do
    servers = servers_from_seeds(state.seeds)

    GenServer.cast(self(), :reconcile)

    put_in(state, [:topology, :servers], servers)
  end

  defp servers_from_seeds(seeds) do
    for addr <- seeds, into: %{} do
      {addr, ServerDescription.defaults(%{address: addr, type: :unknown})}
    end
  end

  defp remove_address(address, state) do
    :ok = Mongo.Events.notify(%ServerClosedEvent{address: address, topology_pid: self()})
    :ok = Monitor.stop(state.monitors[address])

    :ok = case state.connection_pools[address] do
            nil -> :ok
            pid -> GenServer.stop(pid)
          end

    %{state | monitors: Map.delete(state.monitors, address),
      connection_pools: Map.delete(state.connection_pools, address)}
  end

  defp connect_opts_from_address(opts, address) do
    host_opts =
      "mongodb://" <> address
      |> URI.parse
      |> Map.take([:host, :port])
      |> Enum.into([])
      |> rename_key(:host, :hostname)

    opts
    |> Keyword.merge(host_opts)
    |> Keyword.drop([:name])
  end

  defp rename_key(map, original_key, new_key) do
    value = Keyword.get(map, original_key)
    map |> Keyword.delete(original_key) |> Keyword.put(new_key, value)
  end

  defp fetch_arbiters(state) do
    Enum.flat_map(state.topology.servers, fn {_, s} -> s.arbiters end)
  end
end
