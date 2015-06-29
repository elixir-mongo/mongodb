defmodule BSON.Binary do
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
  defstruct [:value]

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
  defstruct [:utc]

  def to_datetime(%BSON.DateTime{utc: utc}) do
    seconds = div(utc, 1000)
    usec = rem(utc, 1000) * 1000
    {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(seconds)
    {date, {hour, min, sec, usec}}
  end

  def to_iso8601(%BSON.DateTime{} = datetime) do
    {{year, month, day}, {hour, min, sec, usec}} = to_datetime(datetime)

    str = zero_pad(year, 4) <> "-" <> zero_pad(month, 2) <> "-" <> zero_pad(day, 2) <> "T" <>
          zero_pad(hour, 2) <> ":" <> zero_pad(min, 2) <> ":" <> zero_pad(sec, 2)

    if is_nil(usec) or usec == 0 do
      str <> "Z"
    else
      str <> "." <> zero_pad(usec, 6) <> "Z"
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
  defstruct [:value]

  defimpl Inspect do
    def inspect(%BSON.Timestamp{value: value}, _opts) do
      "#BSON.Timestamp<#{value}>"
    end
  end
end
