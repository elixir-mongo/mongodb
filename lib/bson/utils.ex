defmodule BSON.Utils do
  defmacro is_int32(value) do
    quote do
      is_integer(unquote(value)) and unquote(value) in @int32_min..@int32_max
    end
  end

  defmacro is_int64(value) do
    quote do
      is_integer(unquote(value)) and unquote(value) in @int64_min..@int64_max
    end
  end
end
