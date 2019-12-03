defmodule Mongo.ConfigHide do
  @default_user    "anonymous"
  @password_masked "***"
  
  def to_options_with_password_masked_if_defined(original_opts) do
    case Map.get(original_opts, :password) do
      nil ->  
        original_opts   
      actual_password  -> 
        original_opts
        |>retain_password_in_env(actual_password)

        original_opts
        |>Map.replace!(:password, @password_masked)
    end
  end
 
  def to_options_list_with_actual_password_if_defined(opts_as_keyword_list) do 
    opts= 
      opts_as_keyword_list
        |> Enum.into(%{}) 

    updated_opts=
      case Map.get(opts, :password) do
        nil ->  
          opts   
        received_password -> 
          case (@password_masked==received_password) do
            true  -> 
              actual_password=retreive_password(opts_as_keyword_list)
              Map.replace!(opts, :password, actual_password )
            false -> 
              opts        
          end  
      end   

    updated_opts
    |> Enum.to_list() 
  end 

  defp retreive_password(opts_as_keylist) do 
    opts=
      opts_as_keylist
      |> Enum.into(%{})  
    {user, hostname, port}=user_hostname_port(opts)
    retreive_password(user, hostname, port)
  end 

  #  
  
  defp retain_password_in_env(opts, password) do 
    {user, hostname, port}=user_hostname_port(opts)
    retain_password_in_env(user, hostname, port, password )
  end 

  defp retain_password_in_env(user, hostname, port, actual_password ) do 
    password_var_name=password_env_var_name(user, hostname, port)
    password_var_name
    |> System.put_env(actual_password) 
  end

  defp retreive_password(user, hostname, port) do 
    password_var_name=password_env_var_name(user, hostname, port)
    password_var_name
    |> System.get_env()
  end

  defp password_env_var_name(user, hostname, port) do 
    port_str= 
      case is_integer(port) do
        true-> 
          Integer.to_string(port)  
        false->
          port 
      end 
    "PASSWORD_FOR_" <>  user <> "_AT_" <>  String.replace( hostname, ".", "_")  <> "_ON_"  <>  port_str
  end

  defp user_hostname_port(opts) do  
    user=Map.get(opts, :user, @default_user )
    hostname=Map.fetch!(opts, :hostname)
    port=Map.fetch!(opts, :port)
    {user, hostname, port}
  end 

end