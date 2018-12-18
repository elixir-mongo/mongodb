defmodule Mongo.TopologyDescription do
  @moduledoc false
  # This acts as a single topology consisting of many connections, built on top
  # of the existing connection API's. It implements the Server Discovery and
  # Monitoring specification, along with the `Mongo.ServerMonitor` module.

  @wire_protocol_range 0..5

  alias Mongo.ServerDescription
  alias Mongo.ReadPreference

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#topologydescription
  @type type :: :unknown | :single | :replica_set_no_primary |
                :replica_set_with_primary | :sharded
  @type t :: %{
    type: type,
    set_name: String.t | nil,
    max_set_version: non_neg_integer | nil,
    max_election_id: BSON.ObjectId.t,
    servers: %{String.t => Mongo.ServerDescription.t},
    compatible: boolean,
    compatibility_error: String.t | nil,
    local_threshold_ms: non_neg_integer
  }

  def defaults(map \\ %{}) do
    default_servers = %{"localhost:27017" => ServerDescription.defaults(%{})}
    Map.merge(%{
      type: :unknown,
      set_name: nil,
      max_set_version: nil,
      max_election_id: nil,
      servers: default_servers,
      compatible: true,
      compatibility_error: nil,
      local_threshold_ms: 15
    }, map)
  end

  def has_readable_server?(_topology, _read_preference) do
    true
  end

  def has_writable_server?(topology) do
    topology.type in [:single, :sharded, :replica_set_with_primary]
  end

  def update(topology, server_description, num_seeds) do
    check_server_supported(topology, server_description, num_seeds)
  end

  # steps 3-4
  def select_servers(topology, type, opts \\ []) do
    read_preference = Keyword.get(opts, :read_preference)
                      |> ReadPreference.defaults()
    if topology[:compatible] == false do
      {:error, :invalid_wire_version}
    else
      {servers, slave_ok, mongos?} = case topology.type do
        :unknown ->
          {[], false, false}
        :single ->
          server =
            topology.servers |> Map.values |> Enum.at(0, %{type: :unknown})
          {topology.servers, type != :write and server.type != :mongos, server.type == :mongos}
        :sharded ->
          mongos_servers =
            topology.servers
            |> Enum.filter(fn {_, server} -> server.type == :mongos end)
          {mongos_servers, false, true}
        _ ->
          case type do
            :read ->
              {select_replica_set_server(topology, read_preference.mode, read_preference), true, false}

            :write ->
              if topology.type == :replica_set_with_primary do
                {select_replica_set_server(topology, :primary, ReadPreference.defaults), false, false}
              else
                {[], false, false}
              end
          end
      end

      servers =
        for {server, _} <- servers do
          server
        end
      {:ok, servers, slave_ok, mongos?}
    end
  end

  ## Private Functions

  defp select_replica_set_server(topology, :primary, _read_preference) do
    Enum.filter(topology.servers, fn {_, server} ->
      server.type == :rs_primary
    end)
  end

  defp select_replica_set_server(topology, :primary_preferred, read_preference) do
    preferred = select_replica_set_server(topology, :primary, read_preference)

    if Enum.empty?(preferred) do
      select_replica_set_server(topology, :secondary, read_preference)
    else
      preferred
    end
  end

  defp select_replica_set_server(topology, :secondary_preferred, read_preference) do
    preferred = select_replica_set_server(topology, :secondary, read_preference)

    if Enum.empty?(preferred) do
      select_replica_set_server(topology, :primary, read_preference)
    else
      preferred
    end
  end

  defp select_replica_set_server(topology, mode, read_preference)
    when mode in [:secondary, :nearest] do
    topology.servers
    |> Enum.filter(fn {_, server} ->
        server.type == :rs_secondary || mode == :nearest
    end)
    |> Enum.into(%{})
    |> filter_out_stale(topology, read_preference.max_staleness_ms)
    |> select_tag_sets(read_preference.tag_sets)
    |> filter_latency_window(topology.local_threshold_ms)
  end

  defp filter_out_stale(servers, topology, max_staleness_ms) do
    if max_staleness_ms == 0 || max_staleness_ms == nil do
      servers
    else
      extra = case topology.type do
        :replica_set_no_primary ->
          {_, server} =
            Enum.reduce(servers, {0, nil}, fn {_, server}, {max, max_server} ->
              if server.last_write_date > max do
                {server.last_write_date, server}
              else
                {max, max_server}
              end
            end)
          server
        :replica_set_with_primary ->
          servers
          |> Enum.filter(fn {_, server} ->
            server.type == :rs_primary
          end)
          |> Enum.at(0)
      end

      servers
      |> Enum.filter(fn {_, server} ->
        case server.type do
          :rs_secondary ->
            case topology.type do
              :replica_set_no_primary ->
                staleness =
                  extra.last_write_date + (server.last_update_time - extra.last_update_time) -
                  server.last_write_date + topology.heartbeat_frequency_ms
                staleness <= max_staleness_ms

              :replica_set_with_primary ->
                staleness =
                  extra.last_write_date - server.last_write_date + topology.heartbeat_frequency_ms
                staleness <= max_staleness_ms
            end
          _ ->
            true
        end
      end)
      |> Enum.into(%{})
    end
  end

  defp select_tag_sets(servers, tag_sets) do
    if Enum.empty?(tag_sets) do
      servers
    else
      tag_sets
      |> Enum.reduce_while(servers, fn (tag_set, servers) ->
        new_servers =
          Enum.filter(servers, fn {_, server} ->
            tag_set_ms = MapSet.new(tag_set)
            server_tag_set_ms = MapSet.new(server.tag_set)
            MapSet.subset?(tag_set_ms, server_tag_set_ms)
          end)
        if Enum.empty?(new_servers) do
          {:cont, servers}
        else
          {:halt, new_servers}
        end
      end)
      |> Enum.into(%{})
    end
  end

  defp filter_latency_window(servers, local_threshold_ms) do
    if Enum.empty?(servers) do
      servers
    else
      min_server =
        servers
        |> Enum.min_by(fn {_, server} ->
          server.round_trip_time
        end)
        |> elem(1)
      latency_window = min_server.round_trip_time + local_threshold_ms

      Enum.filter(servers, fn {_, server} ->
        server.round_trip_time <= latency_window
      end)
    end
  end

  defp check_server_supported(topology, server_description, num_seeds) do
    server_supported_range =
      server_description.min_wire_version ..
      server_description.max_wire_version
    server_supported? = Enum.any?(server_supported_range, fn version ->
      version in @wire_protocol_range
    end)

    if server_supported? do
      check_for_single_topology(topology, server_description, num_seeds)
    else
      topology =
        topology
        |> Map.put(:compatible, false)
        |> Map.put(:compatibility_error,
            "Server at #{server_description.address} uses wire protocol " <>
            "versions #{server_description.min_wire_version} through " <>
            "#{server_description.max_wire_version}, but client only " <>
            "supports #{Enum.min(@wire_protocol_range)} through " <>
            "#{Enum.max(@wire_protocol_range)}.")
      {[], topology}
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#topologytype-single
  defp check_for_single_topology(topology, server_description, num_seeds) do
    case topology.type do
      :single ->
        previous_description =
          topology.servers |> Map.values |> hd
        {[{previous_description, server_description}],
         put_in(topology.servers[server_description.address], server_description)}
      _ ->
        check_server_in_topology(topology, server_description, num_seeds)
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#other-topologytypes
  defp check_server_in_topology(topology, server_description, num_seeds) do
    if not (server_description.address in Map.keys(topology.servers)) do
      {[], topology}
    else
      address = server_description.address
      old_description = topology.servers[address]

      {actions, topology} =
        topology
        |> put_in([:servers, address], server_description)
        |> update_topology(topology.type, server_description, num_seeds)

      {[{old_description, server_description} | actions], topology}
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#topologytype-explanations
  defp update_topology(topology, :unknown, server_description, num_seeds) do
    case server_description.type do
      :unknown ->
        {[], %{topology | type: :unknown}}
      :rs_ghost ->
        {[], %{topology | type: :unknown}}
      :standalone ->
        update_unknown_with_standalone(topology, server_description, num_seeds)
      :mongos ->
        {[], %{topology | type: :sharded}}
      :rs_primary ->
        topology
        |> Map.put(:set_name, server_description.set_name)
        |> update_rs_from_primary(server_description)
      type when type in [:rs_secondary, :rs_arbiter, :rs_other] ->
        topology
        |> Map.put(:set_name, server_description.set_name)
        |> update_rs_without_primary(server_description)
      _ ->
        {[], topology} # don't touch broken states...
    end
  end

  defp update_topology(topology, :sharded, server_description, _) do
    case server_description.type do
      type when type in [:unknown, :mongos] ->
        {[], topology}
      type when type in [:rs_ghost, :standalone, :rs_primary, :rs_secondary,
                         :rs_arbiter, :rs_other] ->
 	      {_, new_topology} = pop_in(topology.servers[server_description.address])
          {[], new_topology}
        _ ->
          {[], topology}
    end
  end

  defp update_topology(topology, :replica_set_no_primary, server_description,
                       _) do
    case server_description.type do
      type when type in [:unknown, :rs_ghost] ->
        {[], topology}
      type when type in [:standalone, :mongos] ->
 	      {_, new_topology} = pop_in(topology.servers[server_description.address])
        {[], new_topology}
      :rs_primary ->
        update_rs_from_primary(topology, server_description)
      type when type in [:rs_secondary, :rs_arbiter, :rs_ghost] ->
        update_rs_without_primary(topology, server_description)
      _ ->
        {[], topology}
    end
  end

  defp update_topology(topology, :replica_set_with_primary, server_description,
                       _) do
    case server_description.type do
      :unknown ->
        topology |> check_if_has_primary
      :rs_ghost ->
        topology |> check_if_has_primary
      type when type in [:standalone, :mongos] ->
	      {_, new_topology} = pop_in(topology.servers[server_description.address])
        check_if_has_primary(new_topology)
      :rs_primary ->
        update_rs_from_primary(topology, server_description)
      type when type in [:rs_secondary, :rs_arbiter, :rs_ghost] ->
        update_rs_with_primary_from_member(topology, server_description)
      _ ->
        {[], topology}
    end
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#actions

  defp not_in_servers?(topology, server_description) do
    not(server_description.address in Map.keys(topology.servers))
  end

  def invalid_set_name?(topology, server_description) do
    topology.set_name != server_description.set_name and
    topology.set_name != nil
  end

  defp update_unknown_with_standalone(topology, server_description, num_seeds) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      if num_seeds == 1 do
        {[], Map.put(topology, :type, :single)}
      else
 	      {_, new_topology} = pop_in(topology.servers[server_description.address])
        {[], new_topology}
      end
    end
  end

  defp update_rs_without_primary(topology, server_description) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      if invalid_set_name?(topology, server_description) do
 	      {_, new_topology} = pop_in(topology.servers[server_description.address])
        {[], new_topology}
      else
        {actions, topology} =
          topology
          |> Map.put(:set_name, server_description.set_name)
          |> add_new_servers(server_description)


        if server_description.address != server_description.me do
          {_, topology} = pop_in(topology.servers[server_description.address])
          {actions, topology}
        else
          {actions, topology}
        end
      end
    end
  end

  defp add_new_servers({actions, topology}, server_description) do
    {[], new_topology} = add_new_servers(topology, server_description)
    {actions, new_topology}
  end

  defp add_new_servers(topology, server_description) do
    all_hosts =
      server_description.hosts ++ server_description.passives ++ server_description.arbiters
    topology = Enum.reduce(all_hosts, topology, fn (host, topology) ->
      if not(host in Map.keys(topology.servers)) do
        # this is kinda like an "upsert"
        put_in(topology.servers[host], ServerDescription.defaults(%{address: host}))
      else
        topology
      end
    end)
    {[], topology}
  end

  defp update_rs_with_primary_from_member(topology, server_description) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      topology = if invalid_set_name?(topology, server_description) do
 	      {_, new_topology} = pop_in(topology.servers[server_description.address])
        new_topology
      else
        topology
      end

      if server_description.address != server_description.me do
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        check_if_has_primary(new_topology)
      else
        if Enum.any?(topology.servers, fn
          {_, server_description} ->
            server_description.type == :rs_primary
        end) do
          {[], topology}
        else
          {[], %{topology | type: :replica_set_no_primary}}
        end
      end
    end
  end

  defp update_rs_from_primary(topology, server_description) do
    if not_in_servers?(topology, server_description) do
      {[], topology}
    else
      if invalid_set_name?(topology, server_description) do
        {_, new_topology} = pop_in(topology.servers[server_description.address])
        check_if_has_primary(new_topology)
      else
        topology
        |> Map.put(:set_name, server_description.set_name)
        |> handle_election_id(server_description)
      end
    end
  end

  defp handle_election_id(topology, server_description) do
    # yes, this is really in the spec
    if server_description[:set_version] != nil and
       server_description[:election_id] != nil do

      has_set_version_and_election_id? =
        topology[:max_set_version] != nil and
        topology[:max_election_id] != nil
      newer_set_version? = topology.max_set_version > server_description.set_version
      same_set_version? = topology.max_set_version == server_description.set_version
      greater_election_id? = topology.max_election_id > server_description.election_id

      if has_set_version_and_election_id? and
         (newer_set_version? or (same_set_version? and greater_election_id?)) do

        new_server_description = ServerDescription.defaults(%{address: server_description.address})

        topology
        |> put_in([:servers, new_server_description.address], new_server_description)
        |> check_if_has_primary
      else
        topology
        |> Map.put(:max_election_id, server_description.election_id)
        |> continue(server_description)
      end
    else
      topology
      |> continue(server_description)
    end
  end

  defp continue(topology, server_description) do
    topology
    |> handle_set_version(server_description)
    |> invalidate_stale_primary(server_description)
    |> add_new_servers(server_description)
    |> remove_dead_nodes(server_description)
    |> check_if_has_primary
  end

  defp handle_set_version(topology, server_description) do
    if server_description.set_version != nil and
       (topology.max_set_version == nil or
        (server_description.set_version > topology.max_set_version)) do
      Map.put(topology, :max_set_version, server_description.set_version)
    else
      topology
    end
  end

  def invalidate_stale_primary(topology, server_description) do
    {actions, new_servers} =
      topology.servers
      |> Enum.reduce({[], %{}}, fn ({address, %{type: type} = server}, {acts, servers}) ->
        if address != server_description.address and type == :rs_primary do
          {[{:force_check, address} | acts],
           Map.put(servers, address, ServerDescription.defaults(%{address: address}))}
        else
          {acts, Map.put(servers, address, server)}
        end
      end)
    {actions, Map.put(topology, :servers, new_servers)}
  end

  def remove_dead_nodes({actions, topology}, server_description) do
    all_hosts =
      server_description.hosts ++ server_description.passives ++ server_description.arbiters

    topology =
      update_in(topology.servers, &Enum.into(Enum.filter(&1, fn {address, _} ->
        address in all_hosts
      end), %{}))

    {actions, topology}
  end

  defp check_if_has_primary({actions, topology}) do
    {[], new_topology} = check_if_has_primary(topology)
    {actions, new_topology}
  end

  defp check_if_has_primary(topology) do
    any_primary? =
      Enum.any?(topology.servers, fn {_, server_description} ->
        server_description.type == :rs_primary
      end)

    if any_primary? do
      {[], %{topology | type: :replica_set_with_primary}}
    else
      {[], %{topology | type: :replica_set_no_primary}}
    end
  end
end
