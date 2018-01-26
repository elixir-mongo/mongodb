defmodule Mongo.EventsTest do

  use ExUnit.Case

  describe "wait_for_notification" do

    alias Mongo.Events.TopologyOpeningEvent, as: TOE
    test "handles timeouts" do
      start = System.monotonic_time(:milliseconds)
      result = Mongo.Events.wait_for_event(TOE, 100)
      assert (System.monotonic_time(:milliseconds) - start) in 80..120
      assert result == :timeout
    end

    test "receives events" do
      event = %TOE{topology_pid: 123}
      spawn(fn -> Mongo.Events.notify(event) end)
      result = Mongo.Events.wait_for_event(TOE, 100)
      assert result == event
    end

  end


end