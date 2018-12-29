defmodule Mongo.PBKDF2Cache do
  @moduledoc false
  use GenServer
  @name __MODULE__

  def start_link(_ \\ nil) do
    GenServer.start_link(__MODULE__, [], name: @name)
  end

  def pbkdf2(password, salt, iterations) do
    GenServer.call(@name, {password, salt, iterations})
  end

  def init([]) do
    {:ok, %{pending: %{}, cache: %{}}}
  end

  def handle_call(key, from, s) do
    cond do
      salted_password = s.cache[key] ->
        {:reply, salted_password, s}
      list = s.pending[key] ->
        {:noreply, put_in(s.pending[key], [from|list])}
      true ->
        _ = run_task(key)
        {:noreply, put_in(s.pending[key], [from])}
    end
  end

  def handle_info({ref, {key, result}}, s) when is_reference(ref) do
    Enum.each(s.pending[key], fn from ->
      GenServer.reply(from, result)
    end)

    s = update_in(s.pending, &Map.delete(&1, key))
    s = put_in(s.cache[key], result)
    {:noreply, s}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :normal}, s) do
    {:noreply, s}
  end

  defp run_task({password, salt, iterations} = key) do
    Task.async(fn ->
      result = Mongo.PBKDF2.generate(password, salt,
                                     iterations: iterations,
                                     length: 20,
                                     digest: :sha)
      {key, result}
    end)
  end
end
