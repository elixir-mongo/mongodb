defmodule BSON.Decoder do
  @moduledoc false
  use BSON.Utils
  alias BSON.Decimal128

  def decode(binary) do
    {map, ""} = document(binary)
    map
  end

  def documents(binary),
    do: documents(binary, [])
  def documents("", acc),
    do: Enum.reverse(acc)
  def documents(binary, acc) do
    {doc, rest} = document(binary)
    documents(rest, [doc|acc])
  end

  defp type(@type_float, <<0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8), rest::binary>>) do
    {:inf, rest}
  end

  defp type(@type_float, <<0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 255::little-integer-size(8), rest::binary>>) do
    {:"-inf", rest}
  end

  defp type(@type_float, <<0, 0, 0, 0, 0, 0, 248::little-integer-size(8), 127::little-integer-size(8), rest::binary>>) do
    {:NaN, rest}
  end

  defp type(@type_float, <<1, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8), rest::binary>>) do
    {:NaN, rest}
  end

  defp type(@type_float, <<float::little-float64, rest::binary>>) do
    {float, rest}
  end

  defp type(@type_string, <<size::int32, rest::binary>>) do
    size = size - 1
    <<string::binary(size), 0x00, rest::binary>> = rest
    {string, rest}
  end

  defp type(@type_document, binary) do
    document(binary)
  end

  defp type(@type_array, binary) do
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

  defp type(@type_datetime, <<unix_ms::int64, rest::binary>>) do
    {DateTime.from_unix!(unix_ms, :millisecond), rest}
  end

  defp type(@type_undefined, rest) do
    {nil, rest}
  end

  defp type(@type_null, rest) do
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

  defp type(@type_symbol, binary) do
    type(@type_string, binary)
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

  defp type(@type_timestamp, <<ordinal::int32, epoch::int32, rest::binary>>) do
    {%BSON.Timestamp{value: epoch, ordinal: ordinal}, rest}
  end

  defp type(@type_int64, <<int::int64, rest::binary>>) do
    {int, rest}
  end

  defp type(@type_decimal128, <<bits::binary-size(16), rest::binary>>) do
    {Decimal128.decode(bits), rest}
  end

  defp type(@type_min, rest) do
    {:BSON_min, rest}
  end

  defp type(@type_max, rest) do
    {:BSON_max, rest}
  end

  def document(<<size::int32, rest::binary>>) do
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
