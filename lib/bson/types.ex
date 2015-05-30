defmodule BSON.Binary do
  defstruct [:binary, :subtype]
end

defmodule BSON.ObjectId do
  defstruct [:value]

  def new(machine_id, proc_id, secs, counter) do
    value = <<secs       :: unsigned-big-32,
              machine_id :: unsigned-big-24,
              proc_id    :: unsigned-big-16,
              counter    :: unsigned-big-24>>
    %BSON.ObjectId{value: value}
  end
end

defmodule BSON.DateTime do
  defstruct [:utc]
end

defmodule BSON.Regex do
  defstruct [:pattern, :options]
end

defmodule BSON.JavaScript do
  defstruct [:code, :scope]
end

defmodule BSON.Timestamp do
  defstruct [:value]
end

defmodule BSON.Keyword do
  @moduledoc false
  defstruct [:list]
end
