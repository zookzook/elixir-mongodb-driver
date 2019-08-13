# CrudExample

This project shows basic CRUD examples. You can use your own MongoDB instance or just use Docker to start a MongoDb container by calling: 

        docker-compose up -d mongodb
        
After that go to the project folder an start

        #> mix deps.get
        #> iex -S mix
        
        iex(1)> CrudExample.example_1()
        iex(1)> CrudExample.example_2()
        
## `example_1`

In this function we are using one connection to the database without using the application supervisor.

## `example_2`

The same operation like `example_1` but now using the connection pooling and application supervisor.

