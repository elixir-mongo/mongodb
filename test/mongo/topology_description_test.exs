defmodule Mongo.TopologyDescriptionTest do
  use ExUnit.Case, async: true
  alias Mongo.{ReadPreference, TopologyDescription}
  import Mongo.TopologyTestData

  test "single server selection" do
    single_server = ["localhost:27017"]

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :secondary})
    ]
    assert {:ok, single_server, true, false} ==
           TopologyDescription.select_servers(single(), :read, opts)

    assert {:ok, single_server, false, false} ==
           TopologyDescription.select_servers(single(), :write)

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :nearest})
    ]
    assert {:ok, single_server, true, false} ==
           TopologyDescription.select_servers(single(), :read, opts)
  end

  test "replica set server selection" do
    all_hosts = ["localhost:27018", "localhost:27019", "localhost:27020"]
    master = "localhost:27018"

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :secondary})
    ]
    assert {:ok, List.delete(all_hosts, master), true, false} ==
           TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :primary})
    ]
    assert {:ok, [master], true, false} ==
           TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :primary_preferred})
    ]
    assert {:ok, [master], true, false} ==
      TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :primary_preferred})
    ]
    assert {:ok, List.delete(all_hosts, master), true, false} ==
      TopologyDescription.select_servers(repl_set_no_master(), :read, opts)


    opts = [
      read_preference: ReadPreference.defaults(%{mode: :nearest})
    ]
    assert {:ok, all_hosts, true, false} ==
           TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :secondary})
    ]
    assert {:ok, List.delete(all_hosts, master), true, false} ==
           TopologyDescription.select_servers(repl_set_no_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :secondary_preferred})
    ]
    assert {:ok, List.delete(all_hosts, master), true, false} ==
      TopologyDescription.select_servers(repl_set_with_master(), :read, opts)

    assert {:ok, [master], true, false} ==
      TopologyDescription.select_servers(repl_set_only_master(), :read, opts)

    assert {:ok, List.delete(all_hosts, master), true, false} ==
           TopologyDescription.select_servers(repl_set_no_master(), :read, opts)

    opts = [
      read_preference: ReadPreference.defaults(%{mode: :nearest})
    ]
    assert {:ok, all_hosts, true, false} ==
           TopologyDescription.select_servers(repl_set_no_master(), :read, opts)

  end

  test "Simplified server selection" do
    single_server = ["localhost:27017"]

    opts = [
      read_preference: %{mode: :secondary}
    ]
    assert {:ok, single_server, true, false} ==
           TopologyDescription.select_servers(single(), :read, opts)
  end
end
