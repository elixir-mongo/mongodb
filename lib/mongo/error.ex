defmodule Mongo.Error do
  defexception [:message, :code]

  def message(e) do
    code = if e.code, do: " #{e.code}"
    "#{e.message}#{code}"
  end

  def exception(tag: :tcp, action: action, reason: reason) do
    formatted_reason = :inet.format_error(reason)
    %Mongo.Error{message: "tcp #{action}: #{formatted_reason} - #{inspect(reason)}"}
  end

  def exception(message: message, code: code) do
    %Mongo.Error{message: message, code: code}
  end

  def exception(message: message) do
    %Mongo.Error{message: message}
  end
end
