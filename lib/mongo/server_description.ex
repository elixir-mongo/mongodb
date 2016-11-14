defmodule Mongo.ServerDescription do
  @moduledoc false

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#serverdescription
  @type type :: :standalone | :mongos | :possible_primary | :rs_primary |
                :rs_secondary | :rs_arbiter | :rs_other | :rs_ghost | :unknown
  @type t :: %{
             address: String.t | nil,
               error: String.t | nil,
     round_trip_time: non_neg_integer | nil,
     last_write_date: BSON.DateTime.t,
             op_time: BSON.ObjectId.t | nil,
                type: type,
    min_wire_version: non_neg_integer, max_wire_version: non_neg_integer,
                  me: String.t | nil,
               hosts: [String.t],
            passives: [String.t],
            arbiters: [String.t],
             tag_set: %{String.t => String.t},
            set_name: String.t | nil,
         set_version: non_neg_integer | nil,
         election_id: BSON.ObjectId.t | nil,
             primary: String.t | nil,
    last_update_time: non_neg_integer
  }

  def defaults(map \\ %{}) do
    Map.merge(%{
               address: "localhost:27017",
                 error: nil,
       round_trip_time: nil,
       last_write_date: nil,
               op_time: nil,
                  type: :unknown,
      min_wire_version: 0,
      max_wire_version: 0,
                    me: nil,
                 hosts: [],
              passives: [],
              arbiters: [],
               tag_set: %{},
              set_name: nil,
           set_version: nil,
           election_id: nil,
               primary: nil,
      last_update_time: 0
    }, map)
  end

  def from_is_master_error(last_server_description, error) do
    defaults(%{
      address: last_server_description.address,
      error: error
    })
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#parsing-an-ismaster-response
  def from_is_master(last_description, rtt, finish_time, is_master_reply) do
    last_rtt = last_description.round_trip_time || rtt

    defaults(%{
      address: last_description.address,
      round_trip_time: round(0.2 * rtt + 0.8 * last_rtt),
      type: determine_server_type(is_master_reply),
      last_write_date: get_in(is_master_reply,
                              ["lastWrite", "lastWriteDate"]),
      op_time: get_in(is_master_reply, ["lastWrite", "opTime"]),
      last_update_time: finish_time,
      min_wire_version: is_master_reply["minWireVersion"] || 0,
      max_wire_version: is_master_reply["maxWireVersion"] || 0,
      me: is_master_reply["me"],
      hosts: (is_master_reply["hosts"] || []) |> Enum.map(&String.downcase/1),
      passives: (is_master_reply["passives"] || [])
                |> Enum.map(&String.downcase/1),
      arbiters: (is_master_reply["arbiters"] || [])
                |> Enum.map(&String.downcase/1),
      tag_set: is_master_reply["tags"] || %{},
      set_name: is_master_reply["setName"],
      set_version: is_master_reply["setVersion"],
      election_id: is_master_reply["electionId"],
      primary: is_master_reply["primary"]
    })
  end

  # see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#type
  defp determine_server_type(is_master_reply) do
    cond do
      is_master_reply["ok"] != 1 ->
        :unknown
      is_master_reply["msg"] == "isdbgrid" ->
        :mongos
      is_master_reply["isreplicaset"] == true ->
        :rs_ghost
      is_master_reply["setName"] != nil ->
        cond do
          is_master_reply["ismaster"] == true ->
            :rs_primary
          is_master_reply["secondary"] == true ->
            :rs_secondary
          is_master_reply["arbiterOnly"] == true ->
            :rs_arbiter
          true ->
            :rs_other
        end
      true ->
        :standalone
    end
  end
end
