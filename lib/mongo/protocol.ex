defmodule Mongo.Protocol do
  import Record

  @op_reply            1
  @op_msg           1000
  @op_update        2001
  @op_insert        2002
  @op_query         2004
  @op_get_more      2005
  @op_delete        2006
  @op_kill_cursors  2007

  defrecord :msg_header, [:length, :request_id, :response_to, :op_code]
  defrecord :op_update, [:header, :coll_name, :flags, :selector, :update]
  defrecord :op_insert, [:flags, :coll_name, :docs]
  defrecord :op_query, [:flags, :coll_name, :num_skip, :num_return, :query, :selector]
  defrecord :op_get_more, [:coll_name, :num_return, :cursor_id]
  defrecord :op_delete, [:coll_name, :flags, :selector]
  defrecord :op_kill_cursors, [:cursor_ids]
  defrecord :op_reply, [:flags, :cursor_id, :from, :num, :docs]
end
