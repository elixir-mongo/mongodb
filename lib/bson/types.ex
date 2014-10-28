defmodule BSON.Binary do
  defstruct [:binary, :subtype]
end

defmodule BSON.ObjectId do
  defstruct [:value]
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
