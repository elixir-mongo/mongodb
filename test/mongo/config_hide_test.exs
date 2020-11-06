defmodule Mongo.HideConfigTest do
  use ExUnit.Case, async: true
  alias Mongo.ConfigHide

  setup_all do
    :ok
  end

  setup do
    :ok
  end

  test "password is defined" do
    opts_list = [hostname: "127.0.0.1", port: 7777, user: "kobil", password: "123"]

    updated_opts_list =
      opts_list
      |> ConfigHide.mask_password()

    {:ok, password_value} =
      updated_opts_list
      |> Keyword.fetch(:password)

    assert :erlang.is_function(password_value)

    updated_opts_list =
      updated_opts_list
      |> ConfigHide.mask_password()

    assert Keyword.equal?(
             opts_list,
             ConfigHide.unmask_password(updated_opts_list)
           )
  end

  test "password is NOT defined" do
    opts_list = [hostname: "127.0.0.1", port: 7777, user: "kobil"]

    updated_opts_list =
      opts_list
      |> ConfigHide.mask_password()

    assert Keyword.equal?(opts_list, updated_opts_list)

    assert Keyword.equal?(
             opts_list,
             ConfigHide.unmask_password(updated_opts_list)
           )
  end
end
