defmodule Mongo.PBKDF2Cache do
  use GenServer
  @name __MODULE__

  def start_link do
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
        run_task(key)
        {:noreply, put_in(s.pending[key], [from])}
    end
  end

  def handle_info({ref, {key, result}}, s) when is_reference(ref) do
    Enum.map(s.pending[key], fn from ->
      GenServer.reply(from, result)
    end)

    s = update_in(s.pending, &Map.delete(&1, key))
    s = put_in(s.cache[key], result)
    {:noreply, s}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :normal}, s) do
    # Just ignore this for now
    {:noreply, s}
  end

  defp run_task({password, salt, iterations} = key) do
    Task.async(fn ->
      {:ok, result} = :pbkdf2.pbkdf2(:sha, password, salt, iterations, 20)
      {key, result}
    end)
  end
end
