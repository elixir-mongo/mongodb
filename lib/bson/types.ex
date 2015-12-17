defmodule BSON.Binary do
  @moduledoc """
  Represents BSON binary type
  """

  defstruct [binary: nil, subtype: :generic]

  defimpl Inspect do
    def inspect(%BSON.Binary{binary: value, subtype: :generic}, _opts) do
      "#BSON.Binary<#{Base.encode16(value, case: :lower)}>"
    end

    def inspect(%BSON.Binary{binary: value, subtype: subtype}, _opts) do
      "#BSON.Binary<#{Base.encode16(value, case: :lower)}, #{subtype}>"
    end
  end
end

defmodule BSON.ObjectId do
  @moduledoc """
  Represents BSON ObjectId type
  """
  defstruct [:value]

  @doc """
  Creates a new ObjectId from the consisting parts
  """
  def new(machine_id, proc_id, secs, counter) do
    value = <<secs       :: unsigned-big-32,
              machine_id :: unsigned-big-24,
              proc_id    :: unsigned-big-16,
              counter    :: unsigned-big-24>>
    %BSON.ObjectId{value: value}
  end

  defimpl Inspect do
    def inspect(%BSON.ObjectId{value: value}, _opts) do
      "#BSON.ObjectId<#{Base.encode16(value, case: :lower)}>"
    end
  end
end

defmodule BSON.DateTime do
  @moduledoc """
  Represents BSON DateTime type
  """

  defstruct [:utc]

  @epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

  @doc """
  Converts `BSON.DateTime` into a `{{year, month, day}, {hour, min, sec, usec}}`
  tuple.
  """
  def to_datetime(%BSON.DateTime{utc: utc}) do
    seconds = div(utc, 1000) + @epoch
    usec = rem(utc, 1000) * 1000
    {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(seconds)
    {date, {hour, min, sec, usec}}
  end

  @doc """
  Converts `{{year, month, day}, {hour, min, sec, usec}}` into a `BSON.DateTime`
  struct.
  """
  def from_datetime({date, {hour, min, sec, usec}}) do
    greg_secs = :calendar.datetime_to_gregorian_seconds({date, {hour, min, sec}})
    epoch_secs = greg_secs - @epoch
    %BSON.DateTime{utc: epoch_secs * 1000 + div(usec, 1000)}
  end

  @doc """
  Converts `BSON.DateTime` to its ISO8601 representation
  """
  def to_iso8601(%BSON.DateTime{} = datetime) do
    {{year, month, day}, {hour, min, sec, usec}} = to_datetime(datetime)

    str = zero_pad(year, 4) <> "-" <> zero_pad(month, 2) <> "-" <> zero_pad(day, 2) <> "T" <>
          zero_pad(hour, 2) <> ":" <> zero_pad(min, 2) <> ":" <> zero_pad(sec, 2)

    case usec do
      0 -> str <> "Z"
      _ -> str <> "." <> zero_pad(usec, 6) <> "Z"
    end
  end

  defp zero_pad(val, count) do
    num = Integer.to_string(val)
    :binary.copy("0", count - byte_size(num)) <> num
  end

  defimpl Inspect do
    def inspect(%BSON.DateTime{} = datetime, _opts) do
      "#BSON.DateTime<#{BSON.DateTime.to_iso8601(datetime)}>"
    end
  end
end

defmodule BSON.Regex do
  @moduledoc """
  Represents BSON Regex type
  """

  defstruct [:pattern, :options]

  defimpl Inspect do
    def inspect(%BSON.Regex{pattern: pattern, options: nil}, _opts) do
      "#BSON.Regex<#{inspect pattern}>"
    end

    def inspect(%BSON.Regex{pattern: pattern, options: options}, _opts) do
      "#BSON.Regex<#{inspect pattern}, #{inspect options}>"
    end
  end
end

defmodule BSON.JavaScript do
  @moduledoc """
  Represents BSON JavaScript (with and without scope) types
  """

  defstruct [:code, :scope]

  defimpl Inspect do
    def inspect(%BSON.JavaScript{code: code, scope: nil}, _opts) do
      "#BSON.JavaScript<#{inspect code}>"
    end

    def inspect(%BSON.JavaScript{code: code, scope: scope}, _opts) do
      "#BSON.JavaScript<#{inspect code}, #{inspect(scope)}>"
    end
  end
end

defmodule BSON.Timestamp do
  @moduledoc """
  Represents BSON Timestamp type
  """

  defstruct [:value]

  defimpl Inspect do
    def inspect(%BSON.Timestamp{value: value}, _opts) do
      "#BSON.Timestamp<#{value}>"
    end
  end
end
