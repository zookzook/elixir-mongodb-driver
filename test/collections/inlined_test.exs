defmodule Collections.InlinedTest do
  use MongoTest.Case, async: true

  defmodule Person do
    use Mongo.Collection

    collection "persons" do
      attribute :name, String.t(), default: "new name"

      embeds_one :friend, Friend, default: &Person.Friend.new/0 do
        attribute :name, String.t(), default: "new friend"
      end

      embeds_one :hobby, Hobby do
        attribute :name, String.t()
      end

      embeds_many :pets, Pet, default: [] do
        attribute :name, String.t(), default: "new pet"
      end

      embeds_many :things, Thing do
        attribute :name, String.t()
      end
    end
  end

  test "created the appropriate modules", _c do
    Code.ensure_compiled!(Person)
    Code.ensure_compiled!(Person.Friend)
    Code.ensure_compiled!(Person.Hobby)
    Code.ensure_compiled!(Person.Pet)
    Code.ensure_compiled!(Person.Thing)
  end

  test "can create a document for an inlined collection", _c do
    new_person = %{
      Person.new()
      | friend: %{Person.Friend.new() | name: "new friend"},
        hobby: %{Person.Hobby.new() | name: "new hobby"},
        pets: [%{Person.Pet.new() | name: "new pet"}],
        things: [%{Person.Thing.new() | name: "new thing"}]
    }

    map_person = Person.dump(new_person)
    struct_person = Person.load(map_person, false)

    assert %{
             "name" => "new name",
             "friend" => %{"name" => "new friend"},
             "hobby" => %{"name" => "new hobby"},
             "pets" => [%{"name" => "new pet"}],
             "things" => [%{"name" => "new thing"}]
           } = map_person

    assert %Person{
             name: "new name",
             hobby: %Person.Hobby{name: "new hobby"},
             friend: %Person.Friend{name: "new friend"},
             things: [%Person.Thing{name: "new thing"}],
             pets: [%Person.Pet{name: "new pet"}]
           } = struct_person

    new_person = Person.new()
    map_person = Person.dump(new_person)
    struct_person = Person.load(map_person, false)
    assert %{"name" => "new name", "friend" => %{"name" => "new friend"}, "pets" => []} = map_person
    assert %Person{name: "new name", friend: %Person.Friend{name: "new friend"}, pets: []} = struct_person
  end
end
