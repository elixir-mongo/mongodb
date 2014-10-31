defmodule BSON.Decoder do
  import BSON.BinaryUtils

  @type_float     0x01
  @type_string    0x02
  @type_map       0x03
  @type_list      0x04
  @type_binary    0x05
  @type_objectid  0x06
  @type_bool      0x08
  @type_datetime  0x09
  @type_nil       0x0A
  @type_regex     0x0B
  @type_js        0x0D
  @type_js_scope  0x0F
  @type_int32     0x10
  @type_timestamp 0x11
  @type_int64     0x12
  @type_min       0x13
  @type_max       0x14

  def decode(binary) do
    {map, ""} = document(binary)
    map
  end

  defp type(@type_float, <<float::float64, rest::binary>>) do
    {float, rest}
  end

  defp type(@type_string, <<size::int32, rest::binary>>) do
    size = size - 1
    <<string::binary(size), 0x00, rest::binary>> = rest
    {string, rest}
  end

  defp type(@type_map, binary) do
    document(binary)
  end

  defp type(@type_list, binary) do
    list(binary)
  end

  defp type(@type_binary, <<size::int32, subtype, binary::binary(size), rest::binary>>) do
    subtype = subtype(subtype)
    {%BSON.Binary{binary: binary, subtype: subtype}, rest}
  end

  defp type(@type_objectid, <<binary::binary(12), rest::binary>>) do
    {%BSON.ObjectId{value: binary}, rest}
  end

  defp type(@type_bool, <<0x00, rest::binary>>) do
    {false, rest}
  end

  defp type(@type_bool, <<0x01, rest::binary>>) do
    {true, rest}
  end

  defp type(@type_datetime, <<utc::int64, rest::binary>>) do
    {%BSON.DateTime{utc: utc}, rest}
  end

  defp type(@type_nil, rest) do
    {nil, rest}
  end

  defp type(@type_regex, binary) do
    {pattern, rest} = cstring(binary)
    {options, rest} = cstring(rest)
    {%BSON.Regex{pattern: pattern, options: options}, rest}
  end

  defp type(@type_js, binary) do
    {code, rest} = type(@type_string, binary)
    {%BSON.JavaScript{code: code}, rest}
  end

  defp type(@type_js_scope, <<size::int32, binary::binary>>) do
    size = size - 4
    <<binary::binary(size), rest::binary>> = binary
    {code, binary} = type(@type_string, binary)
    {scope, ""} = document(binary)
    {%BSON.JavaScript{code: code, scope: scope}, rest}
  end

  defp type(@type_int32, <<int::int32, rest::binary>>) do
    {int, rest}
  end

  defp type(@type_timestamp, <<value::int64, rest::binary>>) do
    {%BSON.Timestamp{value: value}, rest}
  end

  defp type(@type_int64, <<int::int64, rest::binary>>) do
    {int, rest}
  end

  defp type(@type_min, rest) do
    {:BSON_min, rest}
  end

  defp type(@type_max, rest) do
    {:BSON_max, rest}
  end

  defp document(<<size::int32, rest::binary>>) do
    size = size - 5
    <<doc::binary(size), 0x00, rest::binary>> = rest

    {doc_fields(doc, []), rest}
  end

  defp doc_fields(<<type, rest::binary>>, acc) do
    {key, rest} = cstring(rest)
    {value, rest} = type(type, rest)

    doc_fields(rest, [{key, value}|acc])
  end

  defp doc_fields("", acc) do
    acc |> Enum.reverse |> Enum.into(%{})
  end

  defp list(<<size::int32, rest::binary>>) do
    size = size - 5
    <<list::binary(size), 0x00, rest::binary>> = rest

    {list_elems(list, 0, []), rest}
  end

  defp list_elems(<<type, rest::binary>>, ix, acc) do
    ix_string = Integer.to_string(ix)
    {^ix_string, rest} = cstring(rest)
    {value, rest} = type(type, rest)

    list_elems(rest, ix + 1, [value|acc])
  end

  defp list_elems("", _ix, acc) do
    acc |> Enum.reverse
  end

  defp cstring(binary) do
    [string, rest] = :binary.split(binary, <<0x00>>)
    {string, rest}
  end

  defp subtype(:generic),
    do: 0x00
  defp subtype(:function),
    do: 0x01
  defp subtype(:binary_old),
    do: 0x02
  defp subtype(:uuid_old),
    do: 0x03
  defp subtype(:uuid),
    do: 0x04
  defp subtype(:md5),
    do: 0x05
  defp subtype(int) when is_integer(int) and int in 0x80..0xFF,
    do: 0x80

  defp subtype(0x00),
    do: :generic
  defp subtype(0x01),
    do: :function
  defp subtype(0x02),
    do: :binary_old
  defp subtype(0x03),
    do: :uuid_old
  defp subtype(0x04),
    do: :uuid
  defp subtype(0x05),
    do: :md5
  defp subtype(int) when is_integer(int) and int in 0x80..0xFF,
    do: int
end
