defmodule Mongo.Version do
  @moduledoc false

  defstruct [major: "0", minor: "0", patch: "0"]

  def from_string(vsn) do
    case String.split(vsn) do
      [major] ->
        %__MODULE__{major: major}
      [major, minor] ->
        %__MODULE__{major: major, minor: minor}
      [major, minor, patch] ->
        %__MODULE__{major: major, minor: minor, patch: patch}
    end
  end

  def compare(vsn1, vsn2) do
    %{major: major1, minor: minor1, patch: patch1} = vsn1
    %{major: major2, minor: minor2, patch: patch2} = vsn2

    case {{major1, minor1, patch1}, {major2, minor2, patch2}} do
      {first, second} when first > second -> :gt
      {first, second} when first < second -> :lt
      _ -> :eq
    end
  end
end
