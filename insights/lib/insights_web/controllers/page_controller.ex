defmodule InsightsWeb.PageController do
  use InsightsWeb, :controller

  def index(conn, _params) do
    render(conn, "index.html")
  end
end
