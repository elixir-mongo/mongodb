defmodule Mongo.HideConfigTest do

  import ExUnit.CaptureLog
  alias Mongo.ConfigHide, as: ConfigHide
  require ConfigHide

  @password_masked "***" 

  test "password is defined" do
  
    opts_list= [hostname: "127.0.0.1", port: 7777,  user: "kobil", password: "123" ]

    updated_opts_list=
      opts_list
      |>ConfigHide.to_options_with_password_masked_if_defined()
    
    password_value=
      updated_opts_list
      |>Keyword.fetch(:password) 

    assert @password_masked = password_value  

    assert Keyword.equal?(opts_list, ConfigHide.to_options_list_with_actual_password_if_defined(updated_opts_list))  

  end


  test "password is NOT defined" do

    opts_list= [hostname: "127.0.0.1", port: 7777,  user: "kobil" ]

    updated_opts_list=
      opts_list
      |>ConfigHide.to_options_with_password_masked_if_defined()
    
    assert Keyword.equal?(opts_list, updated_opts_list)  

    assert Keyword.equal?(opts_list,ConfigHide.to_options_list_with_actual_password_if_defined(updated_opts_list))

  end

end
