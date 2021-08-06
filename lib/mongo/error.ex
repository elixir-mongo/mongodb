defmodule Mongo.Error do
  defexception [:message, :code, :host, type: :mongo]

  @type t :: %__MODULE__{
          message: String.t(),
          code: number,
          host: String.t()
        }

  def message(e) do
    code = if e.code, do: " #{e.code}"
    "#{e.message}#{code}"
  end

  def exception(tag: :tcp, action: action, reason: reason, host: host) do
    formatted_reason = :inet.format_error(reason)

    %__MODULE__{
      message: "#{host} tcp #{action}: #{formatted_reason} - #{inspect(reason)}",
      type: :network
    }
  end

  def exception(tag: :ssl, action: action, reason: reason, host: host) do
    formatted_reason = :ssl.format_error(reason)

    %__MODULE__{
      message: "#{host} ssl #{action}: #{formatted_reason} - #{inspect(reason)}",
      type: :network
    }
  end

  def exception(message: message, code: code) do
    %__MODULE__{message: message, code: code}
  end

  def exception(message: message) do
    %__MODULE__{message: message}
  end

  @retryable_error_codes [
    # HostUnreachable
    6,
    # HostNotFound
    7,
    # NetworkTimeout
    89,
    # ShutdownInProgress
    91,
    # PrimarySteppedDown
    189,
    # SocketException
    9001,
    # NotMaster
    10107,
    # InterruptedAtShutdown
    11600,
    # InterruptedDueToReplStateChange
    11602,
    # NotMasterNoSlaveOk
    13435,
    # NotMasterOrSecondary
    13436
  ]

  def retryable(%__MODULE__{code: code}) when code in @retryable_error_codes,
    do: true

  def retryable(%__MODULE__{message: message}) do
    message =~ ~r/not master|node is recovering/
  end
end

defmodule Mongo.WriteError do
  defexception [:n, :ok, :write_errors]

  @type t :: %__MODULE__{
    n: integer(),
    ok: integer(),
    write_errors: [map()]
  }

  def message(e) do
    "n: #{e.n}, ok: #{e.ok}, write_errors: #{inspect(e.write_errors)}"
  end
end
