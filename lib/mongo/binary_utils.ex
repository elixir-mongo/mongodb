defmodule Mongo.BinaryUtils do
  @moduledoc false

  defmacro int64 do
    quote do: signed-little-64
  end

  defmacro int32 do
    quote do: signed-little-32
  end

  defmacro int16 do
    quote do: signed-little-16
  end

  defmacro uint16 do
    quote do: unsigned-little-16
  end

  defmacro int8 do
    quote do: signed-little-8
  end

  defmacro float64 do
    quote do: float-little-64
  end

  defmacro float32 do
    quote do: float-little-32
  end

  defmacro binary(size) do
    quote do: binary-size(unquote(size))
  end
end
