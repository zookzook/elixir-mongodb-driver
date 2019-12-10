defmodule Mongo.PasswordSafe do
  @moduledoc """
  The password safe stores the password while parsing the url and/or the options to avoid it from logging while the sasl logger is activated.
  """

  @me __MODULE__

  use GenServer

  def start_link(_ \\ nil) do
    GenServer.start_link(__MODULE__, [], name: @me)
  end

  def set_password(password) do
    GenServer.cast(@me, {:set, password})
  end

  def get_pasword() do
    GenServer.call(@me, :get)
  end

  def init([]) do
    {:ok, nil}
  end

  def handle_cast({:set, password}, data) do
    {:noreply, password}
  end

  def handle_call(:get, _from, password) do
    {:reply, password, password}
  end

end
