defmodule BSON.Utils do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      import BSON.Utils
      import Mongo.BinaryUtils

      @type_float      0x01
      @type_string     0x02
      @type_document   0x03
      @type_array      0x04
      @type_binary     0x05
      @type_undefined  0x06
      @type_objectid   0x07
      @type_bool       0x08
      @type_datetime   0x09
      @type_null       0x0A
      @type_regex      0x0B
      @type_js         0x0D
      @type_symbol     0x0E
      @type_js_scope   0x0F
      @type_int32      0x10
      @type_timestamp  0x11
      @type_int64      0x12
      @type_decimal128 0x13
      @type_min        0xFF
      @type_max        0x7F
    end
  end

  @int32_min -2_147_483_648
  @int32_max  2_147_483_647
  @int64_min -9_223_372_036_854_775_808
  @int64_max  9_223_372_036_854_775_807

  defmacro is_int32(value) do
    quote do
      is_integer(unquote(value))
      and unquote(value) in unquote(@int32_min)..unquote(@int32_max)
    end
  end

  defmacro is_int64(value) do
    quote do
      is_integer(unquote(value))
      and unquote(value) in unquote(@int64_min)..unquote(@int64_max)
    end
  end
end
