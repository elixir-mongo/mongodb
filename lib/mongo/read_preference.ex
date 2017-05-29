defmodule Mongo.ReadPreference do
  @moduledoc ~S"""
  Determines which servers are considered suitable for read operations
  """
  @type t :: %{
    mode: :primary | :secondary | :primary_preferred | :secondary_preferred |
          :nearest,
    tag_sets: [%{String.t => String.t}],
    max_staleness_ms: non_neg_integer
  }

  def defaults(map \\ %{}) do
    Map.merge(%{
      mode: :primary,
      tag_sets: [%{}],
      max_staleness_ms: 0
    }, map)
  end
end
