defmodule Mongo.Protocol do
  import Record
  import Mongo.BinaryUtils
  alias BSON.Encoder
  alias BSON.Decoder

  @op_reply            1
  @op_update        2001
  @op_insert        2002
  @op_query         2004
  @op_get_more      2005
  @op_delete        2006
  @op_kill_cursors  2007

  @flags %{
    upsert:            0x1,
    multiupdate:       0x2,

    continue_on_error: 0x1,

    tailable_cursor:   0x2,
    slave_ok:          0x4,
    oplog_replay:      0x8,
    no_cursor_timeout: 0x10,
    await_data:        0x20,
    exhaust:           0x40,
    partial:           0x80,

    single_remove:     0x1
  }

  @reply_flags %{
    cursor_not_found:   0x1,
    query_failure:      0x2,
    shard_config_stale: 0x4,
    await_capable:      0x8
  }

  @header_size 4 * 4

  defrecordp :msg_header, [:length, :request_id, :response_to, :op_code]
  defrecord  :op_update, [:coll, :flags, :select, :update]
  defrecord  :op_insert, [:flags, :coll, :docs]
  defrecord  :op_query, [:flags, :coll, :num_skip, :num_return, :query, :select]
  defrecord  :op_get_more, [:coll, :num_return, :cursor_id]
  defrecord  :op_delete, [:coll, :flags, :select]
  defrecord  :op_kill_cursors, [:cursor_ids]
  defrecord  :op_reply, [:flags, :cursor_id, :from, :num, :docs]

  def encode(request_id, op) do
    iodata = encode_op(op)
    header = msg_header(length: IO.iodata_length(iodata) + @header_size,
                        request_id: request_id, response_to: 0,
                        op_code: op_to_code(op))

    [encode_header(header)|iodata]
  end

  def decode(binary) do
    case decode_header(binary) do
      {:ok, msg_header(length: length, response_to: response_to), rest}
          when byte_size(rest) >= length - @header_size ->
        reply_size = length - @header_size
        <<reply::binary(reply_size), rest::binary>> = rest
        reply = decode_reply(reply)
        {:ok, response_to, reply, rest}

      _ ->
        :error
    end
  end

  defp encode_op(op_update(coll: coll, flags: flags, select: select, update: update)) do
    [<<0x00::int32>>, coll, <<0x00, blit_flags(flags)::int32>>,
     Encoder.document(select), Encoder.document(update)]
  end

  defp encode_op(op_insert(flags: flags, coll: coll, docs: docs)) do
    [<<blit_flags(flags)::int32>>, coll, 0x00 |
     Enum.map(docs, &Encoder.document/1)]
  end

  defp encode_op(op_query(flags: flags, coll: coll, num_skip: num_skip,
                          num_return: num_return, query: query, select: select)) do
    [<<blit_flags(flags)::int32>>, coll, <<0x00, num_skip::int32,
       num_return::int32>>, Encoder.document(query) |
     maybe(select, &Encoder.document/1)]
  end

  defp encode_op(op_get_more(coll: coll, num_return: num_return, cursor_id: cursor_id)) do
    [<<0x00::int32>>, coll | <<0x00, num_return::int32, cursor_id::int64>>]
  end

  defp encode_op(op_delete(coll: coll, flags: flags, select: select)) do
    [<<0x00::int32>>, coll, <<0x00, blit_flags(flags)::binary>> |
     Encoder.document(select)]
  end

  defp encode_op(op_kill_cursors(cursor_ids: ids)) do
    binary_ids = for id <- ids, do: <<id::int64>>
    num = byte_size(binary_ids) / 8
    [<<0x00::int32, num::int32>> | binary_ids]
  end

  defp op_to_code(op_update()),       do: @op_update
  defp op_to_code(op_insert()),       do: @op_insert
  defp op_to_code(op_query()),        do: @op_query
  defp op_to_code(op_get_more()),     do: @op_get_more
  defp op_to_code(op_delete()),       do: @op_delete
  defp op_to_code(op_kill_cursors()), do: @op_kill_cursors

  defp decode_reply(<<flags::int32, cursor_id::int64, from::int32, num::int32, rest::binary>>) do
    flags = unblit_flags(flags)
    docs = decode_documents(rest, [])
    op_reply(flags: flags, cursor_id: cursor_id, from: from, num: num, docs: docs)
  end

  defp decode_header(<<length::int32, request_id::int32, response_to::int32,
                       op_code::int32, rest::binary>>) do
    header = msg_header(length: length, request_id: request_id,
                        response_to: response_to, op_code: op_code)
    {:ok, header, rest}
  end

  defp decode_header(_) do
    :error
  end

  defp encode_header(msg_header(length: length, request_id: request_id,
                                response_to: response_to, op_code: op_code)) do
    <<length::int32, request_id::int32, response_to::int32, op_code::int32>>
  end

  defp decode_documents("", acc) do
    Enum.reverse(acc)
  end

  defp decode_documents(binary, acc) do
    {doc, rest} = Decoder.document(binary)
    decode_documents(rest, [doc|acc])
  end

  defp blit_flags(flags) do
    import Bitwise
    Enum.reduce(flags, 0x0, &(flag_to_bit(&1) ||| &2))
  end

  defp unblit_flags(bits) do
    import Bitwise
    Enum.reduce(@reply_flags, [], fn {flag, bit}, acc ->
      if (bit &&& bits) == 0,
          do: acc,
        else: [flag|acc]
    end)
  end

  Enum.each(@flags, fn {flag, bit} ->
    defp flag_to_bit(unquote(flag)), do: unquote(bit)
  end)

  defp flag_to_bit(_flag), do: 0x0

  defp maybe(nil, _fun), do: ""
  defp maybe(value, fun), do: fun.(value)
end
