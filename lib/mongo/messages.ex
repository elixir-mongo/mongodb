defmodule Mongo.Messages do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import unquote(__MODULE__)
      @reply_cursor_not_found   0x1
      @reply_query_failure      0x2
      @reply_shard_config_stale 0x4
      @reply_await_capable      0x8
    end
  end

  import Record
  import Mongo.BinaryUtils

  @op_update        2001
  @op_insert        2002
  @op_query         2004
  @op_get_more      2005
  @op_delete        2006
  @op_kill_cursors  2007

  @update_flags [
    upsert: 0x1,
    multi:  0x2
  ]

  @insert_flags [
    continue_on_error: 0x1
  ]

  @query_flags [
    tailable_cursor:   0x2,
    slave_ok:          0x4,
    oplog_replay:      0x8,
    no_cursor_timeout: 0x10,
    await_data:        0x20,
    exhaust:           0x40,
    partial:           0x80
  ]

  @delete_flags [
    single: 0x1
  ]

  @header_size 4 * 4

  defrecordp :msg_header, [:length, :request_id, :response_to, :op_code]
  defrecord  :op_update, [:coll, :flags, :query, :update]
  defrecord  :op_insert, [:flags, :coll, :docs]
  defrecord  :op_query, [:flags, :coll, :num_skip, :num_return, :query, :select]
  defrecord  :op_get_more, [:coll, :num_return, :cursor_id]
  defrecord  :op_delete, [:coll, :flags, :query]
  defrecord  :op_kill_cursors, [:cursor_ids]
  defrecord  :op_reply, [:flags, :cursor_id, :from, :num, :docs]

  def encode(request_id, op) do
    iodata = encode_op(op)
    header = msg_header(length: IO.iodata_length(iodata) + @header_size,
                        request_id: request_id, response_to: 0,
                        op_code: op_to_code(op))

    [encode_header(header)|iodata]
  end

  def decode_message(msg_header(length: length) = header, iolist)
  when is_list(iolist) do
    if IO.iodata_length(iolist) >= length,
      do: decode_message(header, IO.iodata_to_binary(iolist)),
    else: :error
  end
  def decode_message(msg_header(length: length, response_to: response_to), binary)
  when byte_size(binary) >= length do
    <<reply::binary(length), rest::binary>> = binary
    {:ok, response_to, decode_reply(reply), rest}
  end
  def decode_message(_header, _binary) do
    :error
  end

  def decode_header(iolist) when is_list(iolist) do
    if IO.iodata_length(iolist) >= @header_size,
      do: IO.iodata_to_binary(iolist) |> decode_header,
    else: :error
  end
  def decode_header(<<length::int32, request_id::int32, response_to::int32,
                       op_code::int32, rest::binary>>) do
    header = msg_header(length: length-@header_size, request_id: request_id,
                        response_to: response_to, op_code: op_code)
    {:ok, header, rest}
  end
  def decode_header(_binary) do
    :error
  end

  defp encode_op(op_update(coll: coll, flags: flags, query: query, update: update)) do
    [<<0x00::int32>>,
     coll,
     <<0x00, blit_flags(:update, flags)::int32>>,
     query,
     update]
  end

  defp encode_op(op_insert(flags: flags, coll: coll, docs: docs)) do
    [<<blit_flags(:insert, flags)::int32>>,
     coll,
     0x00,
     docs]
  end

  defp encode_op(op_query(flags: flags, coll: coll, num_skip: num_skip,
                          num_return: num_return, query: query, select: select)) do
    [<<blit_flags(:query, flags)::int32>>,
     coll,
     <<0x00, num_skip::int32, num_return::int32>>,
     query,
     select]
  end

  defp encode_op(op_get_more(coll: coll, num_return: num_return, cursor_id: cursor_id)) do
    [<<0x00::int32>>,
     coll,
     <<0x00, num_return::int32, cursor_id::int64>>]
  end

  defp encode_op(op_delete(coll: coll, flags: flags, query: query)) do
    [<<0x00::int32>>,
     coll,
     <<0x00, blit_flags(:delete, flags)::int32>> |
     query]
  end

  defp encode_op(op_kill_cursors(cursor_ids: ids)) do
    binary_ids = for id <- ids, into: "", do: <<id::int64>>
    num = div byte_size(binary_ids), 8
    [<<0x00::int32, num::int32>>, binary_ids]
  end

  defp op_to_code(op_update()),       do: @op_update
  defp op_to_code(op_insert()),       do: @op_insert
  defp op_to_code(op_query()),        do: @op_query
  defp op_to_code(op_get_more()),     do: @op_get_more
  defp op_to_code(op_delete()),       do: @op_delete
  defp op_to_code(op_kill_cursors()), do: @op_kill_cursors

  defp decode_reply(<<flags::int32, cursor_id::int64, from::int32, num::int32, rest::binary>>) do
    op_reply(flags: flags, cursor_id: cursor_id, from: from, num: num, docs: rest)
  end

  defp encode_header(msg_header(length: length, request_id: request_id,
                                response_to: response_to, op_code: op_code)) do
    <<length::int32, request_id::int32, response_to::int32, op_code::int32>>
  end

  defp blit_flags(op, flags) when is_list(flags) do
    import Bitwise
    Enum.reduce(flags, 0x0, &(flag_to_bit(op, &1) ||| &2))
  end
  defp blit_flags(_op, flags) when is_integer(flags) do
    flags
  end

  Enum.each(@update_flags, fn {flag, bit} ->
    defp flag_to_bit(:update, unquote(flag)), do: unquote(bit)
  end)

  Enum.each(@insert_flags, fn {flag, bit} ->
    defp flag_to_bit(:insert, unquote(flag)), do: unquote(bit)
  end)

  Enum.each(@query_flags, fn {flag, bit} ->
    defp flag_to_bit(:query, unquote(flag)), do: unquote(bit)
  end)

  Enum.each(@delete_flags, fn {flag, bit} ->
    defp flag_to_bit(:delete, unquote(flag)), do: unquote(bit)
  end)

  defp flag_to_bit(_op, _flag), do: 0x0
end
