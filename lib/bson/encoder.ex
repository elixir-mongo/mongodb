defmodule BSON.Encoder do
  @moduledoc false
  use BSON.Utils

  def encode(true),
    do: 0x01

  def encode(false),
    do: 0x00

  def encode(nil),
    do: ""

  def encode(:BSON_min),
    do: ""

  def encode(:BSON_max),
    do: ""

  def encode(%BSON.Binary{binary: binary, subtype: subtype}) do
    subtype = subtype(subtype)
    [<<IO.iodata_length(binary)::int32>>, subtype | binary]
  end

  def encode(%BSON.ObjectId{value: <<_::binary(12)>> = value}),
    do: value

  def encode(%DateTime{} = datetime) do
    unix_ms = DateTime.to_unix(datetime, :millisecond)
    <<unix_ms::int64>>
  end

  def encode(%BSON.Regex{pattern: pattern, options: options}),
    do: [cstring(pattern) | cstring(options)]

  def encode(%BSON.JavaScript{code: code, scope: nil}),
    do: encode(code)

  def encode(%BSON.JavaScript{code: code, scope: scope}) do
    iodata = [encode(code), document(scope)]
    size = IO.iodata_length(iodata) + 4
    [<<size::int32>> | iodata]
  end

  def encode(%BSON.Timestamp{value: epoch, ordinal: ordinal}),
    do: <<ordinal::int32, epoch::int32>>

  def encode([]) do
    document([])
  end

  def encode([{_, _} | _] = value) do
    document(value)
  end

  def encode(value) when is_list(value) do
    array(value, 0)
    |> document
  end

  def encode(value) when is_map(value),
    do: document(value)

  def encode(:inf),
    do: <<0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8)>>

  def encode(:"-inf"),
    do: <<0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 255::little-integer-size(8)>>

  def encode(:NaN),
    do: <<0, 0, 0, 0, 0, 0, 248::little-integer-size(8), 127::little-integer-size(8)>>

  def encode(value) when is_atom(value),
    do: encode(Atom.to_string(value))

  def encode(value) when is_binary(value),
    do: [<<byte_size(value)+1::int32>>, value, 0x00]

  def encode(value) when is_float(value),
    do: <<value::little-float64>>

  def encode(value) when is_int32(value),
    do: <<value::int32>>

  def encode(value) when is_int64(value),
    do: <<value::int64>>

  def document(doc) do
    {_, iodata} =
      Enum.reduce(doc, {:unknown, ""}, fn
        {:__struct__, _value}, {:binary, _acc} ->
          invalid_doc(doc)

        {:__struct__, _value}, {_, acc} ->
          {:atom, acc}

        {key, _value}, {:binary, _acc} when is_atom(key) ->
          invalid_doc(doc)

        {key, _value}, {:atom, _acc} when is_binary(key) ->
          invalid_doc(doc)

        {key, value}, {_, acc} ->
          {key_type, key} = key(key)
          type = type(value)
          value = encode(value)
          {key_type, [acc, type, key, value]}
      end)

    [<<IO.iodata_length(iodata)+5::int32>>, iodata, 0x00]
  end

  defp cstring(string), do: [string, 0x00]

  defp key(value) when is_atom(value),
    do: {:atom, cstring(Atom.to_string(value))}
  defp key(value) when is_binary(value),
    do: {:binary, cstring(value)}

  defp array([], _ix),
    do: []
  defp array([hd|tl], ix) when not is_tuple(hd),
    do: [{Integer.to_string(ix), hd} | array(tl, ix+1)]

  defp invalid_doc(doc) do
    message = "invalid document containing atom and string keys: #{inspect doc}"
    raise ArgumentError, message
  end

  defp type(%BSON.Binary{}),                do: @type_binary
  defp type(%BSON.ObjectId{}),              do: @type_objectid
  defp type(%DateTime{}),                   do: @type_datetime
  defp type(%BSON.Regex{}),                 do: @type_regex
  defp type(%BSON.JavaScript{scope: nil}),  do: @type_js
  defp type(%BSON.JavaScript{}),            do: @type_js_scope
  defp type(%BSON.Timestamp{}),             do: @type_timestamp
  defp type(nil),                           do: @type_null
  defp type(:BSON_min),                     do: @type_min
  defp type(:BSON_max),                     do: @type_max
  defp type(:inf),                          do: @type_float
  defp type(:"-inf"),                       do: @type_float
  defp type(:NaN),                          do: @type_float
  defp type(value) when is_boolean(value),  do: @type_bool
  defp type(value) when is_float(value),    do: @type_float
  defp type(value) when is_atom(value),     do: @type_string
  defp type(value) when is_binary(value),   do: @type_string
  defp type(value) when is_map(value),      do: @type_document
  defp type([{_,_}|_]),                     do: @type_document
  defp type(value) when is_list(value),     do: @type_array
  defp type(value) when is_int32(value),    do: @type_int32
  defp type(value) when is_int64(value),    do: @type_int64

  defp subtype(:generic),    do: 0x00
  defp subtype(:function),   do: 0x01
  defp subtype(:binary_old), do: 0x02
  defp subtype(:uuid_old),   do: 0x03
  defp subtype(:uuid),       do: 0x04
  defp subtype(:md5),        do: 0x05
  defp subtype(int) when is_integer(int) and int in 0x80..0xFF, do: 0x80
end
