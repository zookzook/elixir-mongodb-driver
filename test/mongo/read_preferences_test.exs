defmodule Mongo.ReadPreferencesTest do
  use CollectionCase, async: false

  @tag :rs_required
  test "find_one, using read_preferences options", %{pid: top} do
    coll = unique_collection()

    Mongo.insert_one(top, coll, %{name: "Greta", age: 10})
    Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
    Mongo.insert_one(top, coll, %{name: "Waldo", age: 5})
    Mongo.insert_one(top, coll, %{name: "Oskar", age: 3})

    assert {:ok, 4} == Mongo.count(top, coll, %{})

    Process.sleep(1000)

    prefs = %{
      mode: :secondary_preferred,
      max_staleness_ms: 120_000
    }

    assert %{"name" => "Oskar"} == Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs) |> Map.take(["name"])

    prefs = %{
      mode: :secondary,
      max_staleness_ms: 120_000
    }

    assert %{"name" => "Oskar"} == Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs) |> Map.take(["name"])

    prefs = %{
      mode: :primary
    }

    assert %{"name" => "Oskar"} == Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs) |> Map.take(["name"])

    prefs = %{
      mode: :primary_preferred,
      max_staleness_ms: 120_000
    }

    assert %{"name" => "Oskar"} == Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs) |> Map.take(["name"])
  end

  @doc """

  This test case needs a special deployment like this:

    conf = rs.conf();
    conf.members[0].tags = { "dc": "east", "usage": "production" };
    conf.members[1].tags = { "dc": "east", "usage": "reporting" };
    conf.members[2].tags = { "dc": "west", "usage": "production" };
    rs.reconfig(conf);

  """
  @tag :tag_set
  @tag :rs_required
  test "find_one, using read_preferences options, tag_set", %{pid: top, catcher: catcher} do
    coll = unique_collection()

    Mongo.insert_one(top, coll, %{name: "Greta", age: 10})
    Mongo.insert_one(top, coll, %{name: "Tom", age: 13})
    Mongo.insert_one(top, coll, %{name: "Waldo", age: 5})
    Mongo.insert_one(top, coll, %{name: "Oskar", age: 3})

    assert {:ok, 4} == Mongo.count(top, coll, %{})

    Process.sleep(1000)

    prefs = %{
      mode: :secondary,
      max_staleness_ms: 120_000,
      tags: [dc: "west", usage: "production"]
    }

    assert %{"name" => "Oskar"} == Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs) |> Map.take(["name"])

    prefs = %{
      mode: :nearest,
      max_staleness_ms: 120_000,
      tags: [dc: "east", usage: "production"]
    }

    assert %{"name" => "Oskar"} == Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs) |> Map.take(["name"])
    ## this configuration results in an empty selection
    prefs = %{
      mode: :secondary,
      max_staleness_ms: 120_000,
      tags: [dc: "east", usage: "production"]
    }

    assert catch_exit(Mongo.find_one(top, coll, %{name: "Oskar"}, read_preference: prefs, checkout_timeout: 500))
    assert [:checkout_session | _xs] = EventCatcher.empty_selection_events(catcher) |> Enum.map(fn event -> event.action end)
  end
end
