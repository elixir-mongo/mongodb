defmodule Mongo.ReadPreference do
  @type t :: %{
    mode: :primary | :secondary | :primary_preferred | :secondary_preferred |
          :nearest,
    tag_sets: [%{required(String.t) => String.t}],
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
