defmodule Mongo.ConfigHide do
  @moduledoc false

  @default_user "anonymous"
  @password_masked "***"

  def to_options_with_password_masked_if_defined(original_opts_as_keyword_list) do
    case Keyword.get(original_opts_as_keyword_list, :password) do
      nil ->
        original_opts_as_keyword_list

      actual_password ->
        original_opts_as_keyword_list
        |> retain_password_in_env(actual_password)

        original_opts_as_keyword_list
        |> Keyword.replace!(:password, @password_masked)
    end
  end

  def to_options_list_with_actual_password_if_defined(opts_as_keyword_list) do
    case Keyword.get(opts_as_keyword_list, :password) do
      @password_masked ->
        actual_password = retrieve_password(opts_as_keyword_list)
        Keyword.replace!(opts_as_keyword_list, :password, actual_password)

      _ ->
        opts_as_keyword_list
    end
  end

  defp retrieve_password(opts_as_keyword_list) do
    {user, hostname, port} = user_hostname_port(opts_as_keyword_list)
    retrieve_password(user, hostname, port)
  end

  defp retain_password_in_env(opts, password) do
    {user, hostname, port} = user_hostname_port(opts)
    retain_password_in_env(user, hostname, port, password)
  end

  defp retain_password_in_env(user, hostname, port, actual_password) do
    password_var_name = password_env_var_name(user, hostname, port)

    password_var_name
    |> System.put_env(actual_password)
  end

  defp retrieve_password(user, hostname, port) do
    password_var_name = password_env_var_name(user, hostname, port)

    password_var_name
    |> System.get_env()
  end

  defp password_env_var_name(user, hostname, port) do
    port_str =
      case is_integer(port) do
        true ->
          Integer.to_string(port)

        false ->
          String.trim(port)
      end

    "PASSWORD_FOR_#{user}_AT_#{String.replace(hostname, ".", "_")}_ON_#{port_str}"
  end

  defp user_hostname_port(opts_keyword_list) do
    user = Keyword.get(opts_keyword_list, :user, @default_user)
    hostname = Keyword.fetch!(opts_keyword_list, :hostname)
    port = Keyword.fetch!(opts_keyword_list, :port)
    {user, hostname, port}
  end
end
