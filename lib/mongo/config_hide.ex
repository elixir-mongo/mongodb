defmodule Mongo.ConfigHide do
  @moduledoc false

  @spec mask_password(Keyword.t()) ::
          Keyword.t()
  def mask_password(opts) do
    case Keyword.get(opts, :password) do
      nil ->
        opts

      actual_password ->
        opts
        |> Keyword.replace!(:password, fn -> actual_password end)
    end
  end

  @spec unmask_password(Keyword.t()) ::
          Keyword.t()
  def unmask_password(opts) do
    case Keyword.get(opts, :password) do
      nil ->
        opts

      password_masked ->
        Keyword.replace!(opts, :password, password_masked.())
    end
  end
end
