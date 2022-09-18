defmodule Collections.SimpleTest do
  use MongoTest.Case, async: false

  require Logger

  alias Mongo
  alias Mongo.Collection

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect()
    Mongo.drop_database(pid, nil, w: 3)
    {:ok, [pid: pid]}
  end

  defmodule Label do
    use Collection

    document do
      attribute :name, String.t(), default: "warning"
      attribute :color, String.t(), default: :red
      after_load &Label.after_load/1
    end

    def after_load(%Label{color: color} = label) when is_binary(color) do
      %Label{label | color: String.to_existing_atom(color)}
    end

    def after_load(label) do
      label
    end
  end

  defmodule Card do
    use Collection

    @collection nil

    collection "cards" do
      attribute :title, String.t(), default: "new title"
      embeds_one(:label, Label, default: &Label.new/0)
      timestamps(inserted_at: :created, updated_at: :modified)
    end

    def insert_one(%Card{} = card, top) do
      with map <- dump(card),
           {:ok, _} <- Mongo.insert_one(top, @collection, map) do
        :ok
      end
    end

    def find_one(id, top) do
      top
      |> Mongo.find_one(@collection, %{@id => id})
      |> load()
    end
  end

  test "load and dump", _c do
    alias Collections.SimpleTest.Card
    alias Collections.SimpleTest.Label

    new_card = Card.new()
    map_card = Card.dump(new_card)

    assert %{title: "new title", label: %{color: :red, name: "warning"}} = map_card

    struct_card = Card.load(map_card, true)

    assert %Card{label: %Label{color: :red, name: "warning"}} = struct_card
  end

  test "save and find", c do
    alias Collections.SimpleTest.Card
    alias Collections.SimpleTest.Label

    new_card = Card.new()

    assert :ok = Card.insert_one(new_card, c.pid)

    card = Card.find_one(new_card._id, c.pid)

    assert %Card{label: %Label{color: :red, name: "warning"}} = card
  end
end
