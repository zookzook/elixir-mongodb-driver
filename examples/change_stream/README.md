# Change streams

This project shows a change stream example. You need to set up a replica set, because change streams are only available for replica sets and sharded clusters. For more information see 

* https://www.mongodb.com/blog/post/an-introduction-to-change-streams
* https://docs.mongodb.com/manual/changeStreams/

If you creating a new replica set then you need to create the database `db-1` first, before starting the example. Otherwise you will get some errors, because the database which we will observe does not exsists.

The `ChangeStream` module uses a GenServer for observing changes. It spawns a process to consume the documents returned by the change stream cursor:

    pid = spawn(fn -> Enum.each(get_cursor(state), fn doc -> new_doc(doc) end) end) 

While running this process you will receive some message:

* token: you get a token after a while. You can use this token to reconnect to the change stream without getting old change documents again. 
* documents: If data changes, you get a document which describes these changes

Let's start the program with `iex -S mix`:

        iex(2)> 
        16:10:05.018 [info]  Connecting change stream
         
        16:10:05.022 [info]  Receiving new token nil
         
        iex(3)> Mongo.insert_one(:mongo, "http_errors", %{url: "https://elixir-lang.org"})  
        {:ok,
         %Mongo.InsertOneResult{
           acknowledged: true,
           inserted_id: #BSON.ObjectId<5d595c42306a5f0d87ab24e7>
         }}
        iex(4)> 
        16:10:10.509 [info]  Receiving new token %{"_data" => #BSON.Binary<825d595c420000000146645f696400645d595c42306a5f0d87ab24e7005a1004fefbdf8754024c339cd73f510a91db2b04>}
         
        16:10:10.509 [info]  Receiving new document %{"coll" => "http_errors", "db" => "db-1"}
        
        16:10:10.509 [info]  Got http error for url https://elixir-lang.org
        
