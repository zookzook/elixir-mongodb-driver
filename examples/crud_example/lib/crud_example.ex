defmodule CrudExample do

  def create_vcard() do
    %{
      firstname: "Alexander",
      lastname: "Abendroth",
      contact: %{
        email: "alexander.abendroth@campany.de",
        telephone: "+49 111938947373",
        mobile: "+49 222192938383",
        fax: "+49 3332929292"
      },
      addess: %{
        street: "Fasanenweg 5",
        postal_code: "12345",
        city: "Berlin",
        country: "de"
      }
    }
  end

  def example_1() do

    {:ok, top} = Mongo.start_link(url: "mongodb://localhost:27017/db-1")
    
    result = Mongo.insert_one(top, "people", create_vcard())

    IO.puts "#{inspect result}\n"

    result = Mongo.find_one(top, "people", %{})
    IO.puts "#{inspect result}\n"

    result = Mongo.update_one(top, "people", %{lastname: "Abendroth"}, ["$set": ["address.postal_code": "20000"]])
    IO.puts "#{inspect result}\n"

    result = Mongo.find_one(top, "people", %{"contact.email": "alexander.abendroth@campany.de"})
    IO.puts "#{inspect result}\n"

    result = Mongo.delete_one(top, "people", %{lastname: "Abendroth"})
    IO.puts "#{inspect result}\n"

  end

  def example_2() do

    {:ok, %Mongo.InsertOneResult{acknowledged: true, inserted_id: id}} = Mongo.insert_one(:mongo, "people", create_vcard())

    IO.puts "ID is #{inspect id}\n"

    result = Mongo.find_one(:mongo, "people", %{_id: id})
    IO.puts "#{inspect result}\n"

    result = Mongo.update_one(:mongo, "people", %{_id: id}, ["$set": ["address.postal_code": "20000"]])
    IO.puts "#{inspect result}\n"

    result = Mongo.find_one(:mongo, "people",%{_id: id})
    IO.puts "#{inspect result}\n"

    result = Mongo.delete_one(:mongo, "people", %{_id: id})
    IO.puts "#{inspect result}\n"

  end

end
