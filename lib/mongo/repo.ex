defmodule Mongo.Repo do
  @moduledoc """
  Defines a repository.

  A repository serves as a convenience module for a mongodb instance.

  To include the `Mongo.Repo` module in your application, you can put the use macro in your
  app's Repo module.

      defmodule MyApp.Repo do
        use Mongo.Repo,
          otp_app: :my_app,
          topology: :mongo
      end

  With that in place we can configure the Repo:

      config :my_app, MyApp.Repo,
        url: "mongodb://localhost:27017/my-app-dev",
        timeout: 60_000,
        idle_interval: 10_000,
        queue_target: 5_000

  For a complete list of configuration options take a look at `Mongo`.

  Finally we can add the `Mongo` instance to our application supervision tree

      children = [
        # ...
        {Mongo, MyApp.Repo.config()},
        # ...
      ]

  ## Read-only repositories

  To explicitly set a repository as read-only, we can pass in the `:read_only` flag to `use`:

      use Mongo.Repo,
        otp_app: :my_app,
        topology: :mongo,
        read_only: true

  The read-only option will not include any write operation related functions in the module.
  """

  @type t() :: module()

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Mongo.Repo

      @topology opts[:topology] || :mongo
      @otp_app opts[:otp_app]
      @read_only opts[:read_only] || false

      def config() do
        @otp_app
        |> Application.get_env(__MODULE__, [])
        |> Keyword.put_new(:name, @topology)
      end

      unless @read_only do
        def insert(%{__struct__: module} = doc, opts \\ []) do
          collection = module.__collection__(:collection)
          doc = module.timestamps(doc)

          case Mongo.insert_one(@topology, collection, module.dump(doc), opts) do
            {:error, reason} -> {:error, reason}
            {:ok, %{inserted_id: id}} -> {:ok, %{doc | _id: id}}
          end
        end

        def update(%{__struct__: module, _id: id} = doc) do
          collection = module.__collection__(:collection)
          doc = module.timestamps(doc)

          case Mongo.update_one(@topology, collection, %{_id: id}, %{"$set" => module.dump(doc)}, []) do
            {:error, reason} -> {:error, reason}
            {:ok, %{modified_count: _}} -> {:ok, doc}
          end
        end

        def insert_or_update(%{__struct__: module, _id: id} = doc) do
          collection = module.__collection__(:collection)
          doc = module.timestamps(doc)

          case Mongo.update_one(@topology, collection, %{_id: id}, %{"$set" => module.dump(doc)}, upsert: true) do
            {:error, reason} -> {:error, reason}
            {:ok, %{upserted_ids: [id]}} -> {:ok, %{doc | _id: id}}
            {:ok, %{modified_count: _}} -> {:ok, doc}
          end
        end

        def delete(%{__struct__: module, _id: id} = doc, opts \\ []) do
          collection = module.__collection__(:collection)

          case Mongo.delete_one(@topology, collection, %{_id: id}, opts) do
            {:ok, %{deleted_count: 1}} -> {:ok, doc}
            {:ok, %{deleted_count: 0}} -> {:error, :not_found}
            {:error, reason} -> {:error, reason}
          end
        end

        def delete_by(module, filter \\ %{}, opts \\ []) do
          collection = module.__collection__(:collection)

          case Mongo.delete_one(@topology, collection, module.dump_part(filter), opts) do
            {:ok, %{deleted_count: 1}} -> {:ok, filter}
            {:ok, %{deleted_count: 0}} -> {:error, :not_found}
            {:error, reason} -> {:error, reason}
          end
        end

        def insert!(%{__struct__: module} = doc, opts \\ []) do
          collection = module.__collection__(:collection)
          doc = module.timestamps(doc)
          %{inserted_id: id} = Mongo.insert_one!(@topology, collection, module.dump(doc), opts)
          %{doc | _id: id}
        end

        def update!(%{__struct__: module, _id: id} = doc) do
          collection = module.__collection__(:collection)
          doc = module.timestamps(doc)
          Mongo.update_one!(@topology, collection, %{_id: id}, %{"$set" => module.dump(doc)}, [])
          doc
        end

        def insert_or_update!(%{__struct__: module, _id: id} = doc) do
          collection = module.__collection__(:collection)
          doc = module.timestamps(doc)
          update_one_result = Mongo.update_one!(@topology, collection, %{_id: id}, %{"$set" => module.dump(doc)}, upsert: true)

          case update_one_result do
            %{upserted_ids: [id]} -> %{doc | _id: id}
            %{modified_count: _} -> doc
          end
        end

        def delete!(%{__struct__: module, _id: id} = doc, opts \\ []) do
          collection = module.__collection__(:collection)
          delete_result = Mongo.delete_one!(@topology, collection, %{_id: id}, opts)

          case delete_result do
            %{deleted_count: 1} -> doc
            %{deleted_count: 0} -> raise Mongo.Error.exception("Document not found")
          end
        end

        def insert_all(module, entries, opts \\ []) do
          collection = module.__collection__(:collection)

          case Mongo.insert_many(@topology, collection, entries, opts) do
            {:ok, %{inserted_ids: ids}} -> {:ok, length(ids), ids}
            error -> error
          end
        end

        def update_all(module, filter \\ %{}, update, opts \\ []) do
          collection = module.__collection__(:collection)
          Mongo.update_many(@topology, collection, module.dump_part(filter), update, opts)
        end

        def delete_all(module, filter \\ %{}, opts \\ []) do
          collection = module.__collection__(:collection)
          Mongo.delete_many(@topology, collection, module.dump_part(filter), opts)
        end
      end

      def transaction(fun, opts \\ []) do
        Mongo.transaction(@topology, fun, opts)
      end

      def all(module, filter \\ %{}, opts \\ []) do
        collection = module.__collection__(:collection)

        @topology
        |> Mongo.find(collection, filter, opts)
        |> Enum.map(&module.load/1)
      end

      def stream(module, filter \\ %{}, opts \\ []) do
        collection = module.__collection__(:collection)

        @topology
        |> Mongo.find(collection, module.dump_part(filter), opts)
        |> Stream.map(&module.load/1)
      end

      def aggregate(module, pipeline, opts \\ []) do
        collection = module.__collection__(:collection)

        @topology
        |> Mongo.aggregate(collection, pipeline, opts)
        |> Enum.map(&module.load/1)
      end

      def get(module, id, opts \\ []) do
        collection = module.__collection__(:collection)

        @topology
        |> Mongo.find_one(collection, %{_id: id}, opts)
        |> module.load()
      end

      def get_by(module, filter \\ %{}, opts \\ []) do
        collection = module.__collection__(:collection)

        @topology
        |> Mongo.find_one(collection, module.dump_part(filter), opts)
        |> module.load()
      end

      def fetch(module, id, opts \\ []) do
        case get(module, id, opts) do
          nil -> {:error, :not_found}
          doc -> {:ok, doc}
        end
      end

      def fetch_by(module, filter \\ %{}, opts \\ []) do
        case get_by(module, module.dump_part(filter), opts) do
          nil -> {:error, :not_found}
          doc -> {:ok, doc}
        end
      end

      def count(module, filter \\ %{}, opts \\ []) do
        collection = module.__collection__(:collection)
        Mongo.count_documents(@topology, collection, filter, opts)
      end

      def exists?(module, filter \\ %{}) do
        with {:ok, count} <- count(module, filter, limit: 1) do
          count > 0
        end
      end
    end
  end

  @doc """
  Returns the mongo configuration stored in the `:otp_app` environment.
  """
  @callback config() :: Keyword.t()

  @optional_callbacks get: 3, get_by: 3, aggregate: 3, exists?: 2, all: 3, stream: 3, update_all: 4, delete_all: 3

  @doc """
  Returns a single document struct for the collection defined in the given module and bson object id.

  Returns `nil` if no result was found.

  If multiple documents satisfy the query, this method returns the first document
  according to the natural order which reflects the order of documents on the disk.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  ## Example

      MyApp.Repo.get(Post, id)
      MyApp.Repo.get(Post, id, read_concern: %{level: "local"})
  """
  @callback get(module :: module(), id :: BSON.ObjectId.t(), opts :: Keyword.t()) ::
              Mongo.Collection.t() | nil | {:error, any()}

  @doc """
  Returns a single document struct for the collection defined in the given module and query.

  Returns `nil` if no result was found.

  If multiple documents satisfy the query, this method returns the first document
  according to the natural order which reflects the order of documents on the disk.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  ## Example

      MyApp.Repo.get_by(Post, %{title: title})
      MyApp.Repo.get_by(Post, %{title: title}, read_concern: %{level: "local"})
  """
  @callback get_by(module :: module(), query :: BSON.document(), opts :: Keyword.t()) ::
              Mongo.Collection.t() | nil | {:error, any()}

  @doc """
  Selects documents for the collection defined in the given module and returns a list of collection
  structs for the given filter

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  ## Example

      MyApp.Repo.all(Post, %{title: title})
      MyApp.Repo.all(Post, %{title: title}, batch_size: 2)
  """
  @callback all(module :: module(), filter :: BSON.document(), opts :: Keyword.t()) ::
              list(Mongo.Collection.t())

  @doc """
  Selects documents for the collection defined in the given module and returns a stream of collection
  structs for the given filter

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  ## Example

      MyApp.Repo.stream(Post, %{title: title})
      MyApp.Repo.stream(Post, %{title: title}, batch_size: 2)
  """
  @callback stream(module :: module(), filter :: BSON.document(), opts :: Keyword.t()) ::
              Enumerable.t()

  @doc """
  Performs aggregation operation using the aggregation pipeline on the given collection module and returns
  a list of collection structs.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/aggregate/#aggregate)

  ## Example

      MyApp.Repo.aggregate(Post, [
        %{"$match" => %{title: title}},
        %{"$sort" => [{"title", -1}]},
        %{"$limit" => 10}
      ])
  """
  @callback aggregate(module :: module(), pipeline :: BSON.document(), opts :: Keyword.t()) ::
              list(Mongo.Collection.t())

  @doc """
  Returns the count of documents in the given collection module for the given filter.

  ## Options
    * `:limit` - Maximum number of documents to fetch with the cursor
    * `:skip` - Number of documents to skip before returning the first

  ## Example

      MyApp.Repo.count(Post)
  """
  @callback count(module :: module(), filter :: BSON.document(), opts :: Keyword.t()) :: {:ok, integer()}

  @doc """
  Checks whether there are any documents in the given collection module for the given filter.

  Returns a boolean.

  ## Example

      MyApp.Repo.exists?(Post, %{title: title})
  """
  @callback exists?(module :: module(), filter :: BSON.document()) :: boolean()

  @doc """
  Applies the updates for the documents in the given collection module and filter.

  Uses MongoDB update operators to specify the updates. For more information and all options
  please refer to the [MongoDB documentation](https://docs.mongodb.com/manual/reference/command/update/#dbcmd.update)

  ## Example

      MyApp.Repo.update_all(Post, %{}, %{"$set" => %{title: "updated"}})
      MyApp.Repo.update_all(Post, %{title: "old"}, %{"$set" => %{title: "updated"}})
  """
  @callback update_all(module :: module(), filter :: BSON.document(), update :: BSON.document(), opts :: Keyword.t()) ::
              {:ok, Mongo.UpdateResult.t()}

  @doc """
  Deletes all documents for the given collection module and filter.

  For all options see [Options](https://www.mongodb.com/docs/manual/reference/command/delete/#dbcmd.delete)

  ## Example

      MyApp.Repo.delete_all(Post, %{})
      MyApp.Repo.delete_all(Post, %{title: "todelete"})
  """
  @callback delete_all(module :: module(), filter :: BSON.document(), opts :: Keyword.t()) ::
              {:ok, Mongo.DeleteResult.t()}

  @optional_callbacks fetch: 3, fetch_by: 3

  @doc """
  Returns a single document struct for the collection defined in the given module and bson object id as
  a tuple of `{:ok, document}`.

  Returns `{:error, :not_found}` if no result was found.

  If multiple documents satisfy the query, this method returns the first document
  according to the natural order which reflects the order of documents on the disk.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  ## Example

      MyApp.Repo.fetch(Post, id)
      MyApp.Repo.fetch(Post, id, read_concern: %{level: "local"})
  """
  @callback fetch(module :: module(), id :: BSON.ObjectId.t(), opts :: Keyword.t()) ::
              {:ok, Mongo.Collection.t()} | {:error, :not_found} | {:error, any()}

  @doc """
  Returns a single document struct for the collection defined in the given module and query as
  a tuple of `{:ok, document}`.

  Returns `{:error, :not_found}` if no result was found.

  If multiple documents satisfy the query, this method returns the first document
  according to the natural order which reflects the order of documents on the disk.

  For all options see [Options](https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find)

  ## Example

      MyApp.Repo.fetch_by(Post, %{title: title})
      MyApp.Repo.fetch_by(Post, %{title: title}, read_concern: %{level: "local"})
  """
  @callback fetch_by(module :: module(), query :: BSON.document(), opts :: Keyword.t()) ::
              {:ok, Mongo.Collection.t()} | {:error, :not_found} | {:error, any()}

  @optional_callbacks insert: 2, insert!: 2, update: 1, update!: 1, insert_or_update: 1, insert_or_update!: 1, delete: 2, delete!: 2, insert_all: 3

  @doc """
  Inserts a new document struct into the database and returns a `{:ok, doc}` tuple.

  For all options see [Options](https://www.mongodb.com/docs/manual/reference/method/db.collection.insertOne/#mongodb-method-db.collection.insertOne)

  ## Example

      MyApp.Repo.insert(Post.new())
  """
  @callback insert(doc :: Mongo.Collection.t(), opts :: Keyword.t()) :: {:ok, Mongo.Collection.t()} | {:error, any()}

  @doc """
  Same as `Mongo.Repo.insert/2` but raises an error.
  """
  @callback insert!(doc :: Mongo.Collection.t(), opts :: Keyword.t()) :: Mongo.Collection.t()

  @doc """
  Updates a document struct and returns a `{:ok, doc}` tuple.

  ## Example

      post = MyApp.Repo.insert!(Post.new())
      MyApp.Repo.update(%{post | title: "new"})
  """
  @callback update(doc :: Mongo.Collection.t()) :: {:ok, Mongo.Collection.t()} | {:error, any()}

  @doc """
  Same as `Mongo.Repo.update/1` but raises an error.
  """
  @callback update!(doc :: Mongo.Collection.t()) :: Mongo.Collection.t()

  @doc """
  Upserts a document struct and returns a `{:ok, doc}` tuple.

  ## Example

      MyApp.Repo.insert_or_update(Post.new())
  """
  @callback insert_or_update(doc :: Mongo.Collection.t()) :: {:ok, Mongo.Collection.t()} | {:error, any()}

  @doc """
  Same as `Mongo.Repo.insert_or_update/1` but raises an error.
  """
  @callback insert_or_update!(doc :: Mongo.Collection.t()) :: Mongo.Collection.t()

  @doc """
  Deletes a document struct from the database and returns a `{:ok, doc}` tuple.

  For all options see [Options](https://www.mongodb.com/docs/manual/reference/method/db.collection.deleteOne/)

  ## Example

      MyApp.Repo.delete(post)
  """
  @callback delete(doc :: Mongo.Collection.t(), opts :: Keyword.t()) :: {:ok, Mongo.Collection.t()} | {:error, any()}

  @doc """
  Same as `Mongo.Repo.delete/2` but raises an error.
  """
  @callback delete!(doc :: Mongo.Collection.t(), opts :: Keyword.t()) :: Mongo.Collection.t()

  @doc """
  Inserts all given documents into the document in one write operation and returns an `:ok` tuple
  with the count of inserted documents as second element and the inserted ids as third element.

  Returns `{:error, reason}` on failure.

  ## Example

      MyApp.Repo.insert_all(Post, [%{title: "a"}, %{title: "b"}])
  """
  @callback insert_all(module :: module(), entries :: list(BSON.document()), opts :: Keyword.t()) :: {:ok, integer(), list(BSON.ObjectId.t())} | {:error, any()}

  @doc """
  Convenient function for running multiple write commands in a transaction.

  In case of `TransientTransactionError` or `UnknownTransactionCommitResult` the function will retry the whole transaction or
  the commit of the transaction. You can specify a timeout (`:transaction_retry_timeout_s`) to limit the time of repeating.
  The default value is 120 seconds. If you don't wait so long, you call `transaction` with the
  option `transaction_retry_timeout_s: 10`. In this case after 10 seconds of retrying, the function will return
  an error.

  You can nest the function calls. In this case the first session will be reused.
  """
  @callback transaction(fun :: (... -> {:ok, any()} | :error), opts :: Keyword.t()) :: {:ok, any()} | :error | {:error, any()}
end
