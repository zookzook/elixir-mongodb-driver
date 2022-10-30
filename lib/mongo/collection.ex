defmodule Mongo.Collection do
  @moduledoc """

  This module provides some boilerplate code for a better support of structs while using the
  MongoDB driver:

    * automatic load and dump function
    * reflection functions
    * type specification
    * support for embedding one and many structs
    * support for `after load` function
    * support for `before dump` function
    * support for id generation
    * support for default values
    * support for derived values

  When using the MongoDB driver only maps and keyword lists are used to
  represent documents.
  If you would prefer to use structs instead of the maps to give the document a stronger meaning or to emphasize
  its importance, you have to create a `defstruct` and fill it from the map manually:

      defmodule Label do
        defstruct name: "warning", color: "red"
      end

      iex> label_map = Mongo.find_one(:mongo, "labels", %{})
      %{"name" => "warning", "color" => "red"}
      iex> label = %Label{name: label_map["name"], color: label_map["color"]}

  We have defined a module `Label` as `defstruct`, then we get the first label document
  the collection `labels`. The function `find_one` returns a map. We convert the map manually and
  get the desired struct.

  If we want to save a new structure, we have to do the reverse. We convert the struct into a map:

      iex> label = %Label{}
      iex> label_map = %{"name" => label.name, "color" => label.color}
      iex> {:ok, _} = Mongo.insert_one(:mongo, "labels", label_map)

  Alternatively, you can also remove the `__struct__` key from `label`. The MongoDB driver automatically
  converts the atom keys into strings.

      iex>  Map.drop(label, [:__struct__])
      %{color: :red, name: "warning"}

  If you use nested structures, the work becomes a bit more complex. In this case, you have to use the inner structures
  convert manually, too.

  If you take a closer look at the necessary work, two basic functions can be derived:

    * `load` Conversion of the map into a struct.
    * `dump` Conversion of the struct into a map.

  This module provides the necessary macros to automate this boilerplate code.
  The above example can be rewritten as follows:

      defmodule Label do

        use Collection

        document do
          attribute :name, String.t(), default: "warning"
          attribute :color, String.t(), default: :red
        end

      end

  This results in the following module:

      defmodule Label do

        defstruct [name: "warning", color: "red"]

        @type t() :: %Label{String.t(), String.t()}

        def new()...
        def load(map)...
        def dump(%Label{})...
        def __collection__(:attributes)...
        def __collection__(:types)...
        def __collection__(:collection)...
        def __collection__(:id)...

      end

  You can now create new structs with the default values and use the conversion functions between maps and
  structs:

      iex(1)> x = Label.new()
      %Label{color: :red, name: "warning"}
      iex(2)> m = Label.dump(x)
      %{color: :red, name: "warning"}
      iex(3)> Label.load(m, true)
      %Label{color: :red, name: "warning"}

  The `load/2` function distinguishes between keys of type binarys `load(map, false)` and keys of type atoms `load(map, true)`.
  The default is `load(map, false)`:

      iex(1)> m = %{"color" => :red, "name" => "warning"}
      iex(2)> Label.load(m)
      %Label{color: :red, name: "warning"}

  If you would now expect atoms as keys, the result of the conversion is not correct in this case:

      iex(3)> Label.load(m, true)
      %Label{color: nil, name: nil}

  The background is that MongoDB always returns binarys as keys and structs use atoms as keys.

  ## Default and derived values

  Attributes have two options:

  * `default:` a value or a function, which is called, when a new struct is created
  * `derived:` `true` to indicate, that is attribute should not be saved to the database

  If you call `new/0` a new struct is returned filled with the default values. In case of a function the
  function is called to use the return value as default.

        attribute: created, DateTime.t(), &DateTime.utc_now/0

  If you mark an attribute as a derived attribute (`derived: true`) then the dump function will remove
  the attributes from the struct automatically for you, so these kind of attributes won't be saved in
  the database.

        attribute :id, String.t(), derived: true

  ## Collections

  In MongoDB, documents are written in collections. We can use the `collection/2` macro to create
  a collection:

        defmodule Card do

          use Collection

          @collection nil

          collection "cards" do
            attribute :title, String.t(), default: "new title"
          end

        end

  The `collection/2` macro creates a collection that is basically similar to a document, where
  an attribute for the ID is added automatically. Additionally the attribute `@collection` is assigned and
  can be used as a constant in other functions.

  In the example above we only suppress a warning of the editor by `@collection`. The macro creates the following
  expression: `@collection "cards"`. By default, the following attribute is created for the ID:

      {:_id, BSON.ObjectId.t(), &Mongo.object_id/0}

  where the default value is created via the function `&Mongo.object_id/0` when calling `new/0`:

        iex> Card.new()
        %Card{_id: #BSON.ObjectId<5ec3d04a306a5f296448a695>, title: "new title"}

  Two additional reflection features are also provided:

        iex> Card.__collection__(:id)
        :_id
        iex(3)> Card.__collection__(:collection)
        "cards"

  ## MongoDB example

  We define the following collection:

        defmodule Card do

          use Collection

          @collection nil ## keeps the editor happy
          @id nil

          collection "cards" do
            attribute :title, String.t(), default: "new title"
          end

          def insert_one(%Card{} = card) do
            with map <- dump(card),
                 {:ok, _} <- Mongo.insert_one(:mongo, @collection, map) do
              :ok
            end
          end

          def find_one(id) do
            :mongo
            |> Mongo.find_one(@collection, %{@id => id})
            |> load()
          end

        end

  Then we can call the functions `insert_one` and `find_one`. Thereby
  we always use the defined structs as parameters or get the
  struct as result:

      iex(1)> card = Card.new()
      %Card{_id: #BSON.ObjectId<5ec3ed0d306a5f377943c23c>, title: "new title"}
      iex(6)> Card.insert_one(card)
      :ok
      iex(2)> Card.find_one(card._id)
      %XCard{_id: #BSON.ObjectId<5ec3ecbf306a5f3779a5edaa>, title: "new title"}

  ## Id generator

  In MongoDB it is common to use the attribute `_id` as id. The value is
  uses an ObjectId generated by the mongodb driver. This behavior can be specified by
  the module attribute `@id_generator` when using `collection`.
  The default setting is

        {:_id, BSON.ObjectId.t(), &Mongo.object_id/0}

  Now you can overwrite this tuple `{name, type, function}` as you like:

        @id_generator false # no ID creation
        @id_generator {id, String.t, &IDGenerator.next()/0} # customized name and generator
        @id_generator nil # use default: {:_id, BSON.ObjectId.t(), &Mongo.object_id/0}

  ### Embedded documents

  Until now we had only shown simple attributes. It will only be interesting when we
  embed other structs. With the macros `embeds_one/3` and `embeds_many/3`, structs can be
  added to the attributes:

  ## Example `embeds_one`

        defmodule Label do

          use Collection

          document do
            attribute :name, String.t(), default: "warning"
            attribute :color, String.t(), default: :red
          end

        end

        defmodule Card do

          use Collection

          collection "cards" do
            attribute   :title, String.t()
            attribute   :list, BSON.ObjectId.t()
            attribute   :created, DateString.t(), default: &DateTime.utc_now/0
            attribute   :modified, DateString.t(), default: &DateTime.utc_now/0
            embeds_one  :label, Label, default: &Label.new/0
          end

        end

  If we now call `new/0`, we get the following structure:

        iex(1)> Card.new()
        %Card{
          _id: #BSON.ObjectId<5ec3f0f0306a5f3aa5418a24>,
          created: ~U[2020-05-19 14:45:04.141044Z],
          label: %Label{color: :red, name: "warning"},
          list: nil,
          modified: ~U[2020-05-19 14:45:04.141033Z],
          title: nil
        }


  ## `after_load/1` and `before_dump/1` macros

  Sometimes you may want to perform post-processing after loading the data set, for example
  to create derived attributes. Conversely, before saving, you may want to
  drop the derived attributes so that they are not saved to the database.

  For this reason there are two macros `after_load/1` and `before_dump/1`. You can
  specify functions that are called after the `load/0` or before the `dump`:

  ## Example `embeds_many`

        defmodule Board do

          use Collection

          collection "boards" do

            attribute   :id, String.t() ## derived attribute
            attribute   :title, String.t()
            attribute   :created, DateString.t(), default: &DateTime.utc_now/0
            attribute   :modified, DateString.t(), default: &DateTime.utc_now/0
            embeds_many :lists, BoardList

            after_load  &Board.after_load/1
            before_dump &Board.before_dump/1
          end

          def after_load(%Board{_id: id} = board) do
            %Board{board | id: BSON.ObjectId.encode!(id)}
          end

          def before_dump(board) do
            %Board{board | id: nil}
          end

          def new(title) do
            new()
            |> Map.put(:title, title)
            |> Map.put(:lists, [])
            |> after_load()
          end

          def store(board) do
            with map <- dump(board),
                {:ok, _} <- Mongo.insert_one(:mongo, @collection, map) do
              :ok
            end
          end

          def fetch(id) do
            :mongo
            |> Mongo.find_one(@collection, %{@id => id})
            |> load()
          end

        end

  In this example the attribute `id` is derived from the actual ID and stored as a binary.
  This attribute is often used and therefore we want to save the conversion of the ID.
  To avoid storing the derived attribute `id`, the `before_dump/1` function is called, which
  removes the `id` from the struct:

        iex(1)> board = Board.new("Vega")
        %Board{
          _id: #BSON.ObjectId<5ec3f802306a5f3ee3b71cf2>,
          created: ~U[2020-05-19 15:15:14.374556Z],
          id: "5ec3f802306a5f3ee3b71cf2",
          lists: [],
          modified: ~U[2020-05-19 15:15:14.374549Z],
          title: "Vega"
        }
        iex(2)> Board.store(board)
        :ok
        iex(3)> Board.fetch(board._id)
        %Board{
          _id: #BSON.ObjectId<5ec3f802306a5f3ee3b71cf2>,
          created: ~U[2020-05-19 15:15:14.374Z],
          id: "5ec3f802306a5f3ee3b71cf2",
          lists: [],
          modified: ~U[2020-05-19 15:15:14.374Z],
          title: "Vega"
        }

  If we call the document in the Mongo shell, we see that the attribute `id` was not stored there:

        > db.boards.findOne({"_id" : ObjectId("5ec3f802306a5f3ee3b71cf2")})
        {
          "_id" : ObjectId("5ec3f802306a5f3ee3b71cf2"),
          "created" : ISODate("2020-05-19T15:15:14.374Z"),
          "lists" : [ ],
          "modified" : ISODate("2020-05-19T15:15:14.374Z"),
          "title" : "Vega"
        }
  ## Example `timestamps`

        defmodule Post do

          use Mongo.Collection

          collection "posts" do
            attribute :title, String.t()
            timestamps()
          end

          def new(title) do
            Map.put(new(), :title, title)
          end

          def store(post) do
            MyRepo.insert_or_update(post)
          end
        end

  In this example the macro `timestamps` is used to create two DateTime attributes, `inserted_at` and `updated_at`.
  This macro is intented to use with the Repo module, as it will be responsible for updating the value of `updated_at` attribute before execute the action.

        iex(1)> post = Post.new("lorem ipsum dolor sit amet")
        %Post{
          _id: #BSON.ObjectId<6327a7099626f7f61607e179>,
          inserted_at: ~U[2022-09-18 23:17:29.087092Z],
          title: "lorem ipsum dolor sit amet",
          updated_at: ~U[2022-09-18 23:17:29.087070Z]
        }

        iex(2)> Post.store(post)
        {:ok,
         %{
           _id: #BSON.ObjectId<6327a7099626f7f61607e179>,
           inserted_at: ~U[2022-09-18 23:17:29.087092Z],
           title: "lorem ipsum dolor sit amet",
           updated_at: ~U[2022-09-18 23:19:24.516648Z]
         }}

  Is possible to change the field names, like Ecto does, and also change the default behaviour:

        defmodule Comment do

          use Mongo.Collection

          collection "comments" do
            attribute :text, String.t()
            timestamps(inserted_at: :created, updated_at: :modified, default: &__MODULE__.truncated_date_time/0)
          end

          def new(text) do
            Map.put(new(), :text, text)
          end

          def truncated_date_time() do
            utc_now = DateTime.utc_now()
            DateTime.truncate(utc_now, :second)
          end
        end

        iex(1)> comment = Comment.new("useful comment")
        %Comment{
          _id: #BSON.ObjectId<6327aa979626f7f616ae5b4a>,
          created: ~U[2022-09-18 23:32:39Z],
          modified: ~U[2022-09-18 23:32:39Z],
          text: "useful comment"
        }

        iex(2)> {:ok, comment} = MyRepo.insert(comment)
        {:ok,
         %Comment{
           _id: #BSON.ObjectId<6327aa979626f7f616ae5b4a>,
           created: ~U[2022-09-18 23:32:39Z],
           modified: ~U[2022-09-18 23:32:42Z],
           text: "useful comment"
         }}

        iex(3)> {:ok, comment} = MyRepo.update(%{comment | text: "not so useful comment"})
        {:ok,
         %Comment{
           _id: #BSON.ObjectId<6327aa979626f7f616ae5b4a>,
           created: ~U[2022-09-18 23:32:39Z],
           modified: ~U[2022-09-18 23:32:46Z],
           text: not so useful comment"
         }}

  The `timestamps` macro has some limitations as it does not run in batch commands like `insert_all` or `update_all`, nor does it update embedded documents.

  """

  @type t() :: struct()

  alias Mongo.Collection

  @doc false
  defmacro __using__(_) do
    quote do
      @before_dump_fun &Function.identity/1
      @after_load_fun &Function.identity/1
      @id_generator nil

      import Collection, only: [document: 1, collection: 2]

      Module.register_attribute(__MODULE__, :attributes, accumulate: true)
      Module.register_attribute(__MODULE__, :derived, accumulate: true)
      Module.register_attribute(__MODULE__, :timestamps, accumulate: true)
      Module.register_attribute(__MODULE__, :types, accumulate: true)
      Module.register_attribute(__MODULE__, :embed_ones, accumulate: true)
      Module.register_attribute(__MODULE__, :embed_manys, accumulate: true)
      Module.register_attribute(__MODULE__, :after_load_fun, [])
      Module.register_attribute(__MODULE__, :before_dump_fun, [])
    end
  end

  @doc """
  Defines a struct as a collection with id generator and a collection.

  Inside a collection block, each attribute is defined through the `attribute/3` macro.
  """
  defmacro collection(name, do: block) do
    make_collection(name, block)
  end

  @doc """
  Defines a struct as a document without id generator and a collection. These documents
  are used to be embedded within collection structs.

  Inside a document block, each attribute is defined through the `attribute/3` macro.
  """
  defmacro document(do: block) do
    make_collection(nil, block)
  end

  defp make_collection(name, block) do
    prelude =
      quote do
        @collection unquote(name)

        @id_generator (case @id_generator do
                         nil -> {:_id, quote(do: BSON.ObjectId.t()), &Mongo.object_id/0}
                         false -> {nil, nil, nil}
                         other -> other
                       end)

        @id elem(@id_generator, 0)

        Collection.__id__(@id_generator, @collection)

        try do
          import Collection
          unquote(block)
        after
          :ok
        end
      end

    postlude =
      quote unquote: false do
        attribute_names = @attributes |> Enum.reverse() |> Enum.map(&elem(&1, 0))

        struct_attrs =
          (@attributes |> Enum.reverse() |> Enum.map(fn {name, opts} -> {name, opts[:default]} end)) ++
            (@embed_ones |> Enum.map(fn {name, _mod, opts} -> {name, opts[:default]} end)) ++
            (@embed_manys |> Enum.map(fn {name, _mod, opts} -> {name, opts[:default]} end))

        defstruct struct_attrs

        Collection.__type__(@types)

        def __collection__(:attributes), do: unquote(attribute_names)
        def __collection__(:timestamps), do: unquote(@timestamps)
        def __collection__(:types), do: @types
        def __collection__(:collection), do: unquote(@collection)
        def __collection__(:id), do: unquote(elem(@id_generator, 0))
      end

    new_function =
      quote unquote: false do
        embed_ones = @embed_ones |> Enum.map(fn {name, _mod, opts} -> {name, opts} end)
        embed_manys = @embed_manys |> Enum.map(fn {name, _mod, opts} -> {name, opts} end)

        args =
          (@attributes ++ embed_ones ++ embed_manys)
          |> Enum.map(fn {name, opts} -> {name, opts[:default]} end)
          |> Enum.filter(fn {_name, fun} -> is_function(fun) end)

        def new do
          if @timestamps != [],
            do: new_timestamps(%__MODULE__{unquote_splicing(Collection.struct_args(args))}),
            else: %__MODULE__{unquote_splicing(Collection.struct_args(args))}
        end
      end

    load_function =
      quote unquote: false do
        attribute_names = Enum.map(@attributes, fn {name, opts} -> {name, opts[:name]} end)

        embed_ones =
          @embed_ones
          |> Enum.filter(fn {_name, mod, _opts} -> Collection.has_load_function?(mod) end)
          |> Enum.map(fn {name, mod, opts} -> {name, {opts[:name], mod}} end)

        embed_manys =
          @embed_manys
          |> Enum.filter(fn {_name, mod, _opts} -> Collection.has_load_function?(mod) end)
          |> Enum.map(fn {name, mod, opts} -> {name, {opts[:name], mod}} end)

        def load(_map, _use_atoms \\ false)
        def load(nil, _use_atoms), do: nil
        def load(xs, use_atoms) when is_list(xs), do: Enum.map(xs, fn map -> load(map, use_atoms) end)

        def load(map, use_atoms) when is_map(map) do
          struct = Enum.reduce(unquote(attribute_names), %__MODULE__{}, fn {name, src_name}, result -> Map.put(result, name, map[src_name(src_name, use_atoms)]) end)

          struct =
            unquote(embed_ones)
            |> Enum.map(fn {name, {src_name, mod}} -> {name, mod.load(map[src_name(src_name, use_atoms)], use_atoms)} end)
            |> Enum.reduce(struct, fn {name, doc}, acc -> Map.put(acc, name, doc) end)

          unquote(embed_manys)
          |> Enum.map(fn {name, {src_name, mod}} -> {name, mod.load(map[src_name(src_name, use_atoms)], use_atoms)} end)
          |> Enum.reduce(struct, fn {name, doc}, acc -> Map.put(acc, name, doc) end)
          |> @after_load_fun.()
        end

        defp src_name({src_name, _}, true), do: src_name
        defp src_name({_, src_name}, _), do: src_name
      end

    dump_function =
      quote unquote: false do
        attribute_names = Enum.map(@attributes, fn {name, opts} -> {name, opts[:name]} end)

        embed_ones =
          @embed_ones
          |> Enum.filter(fn {_name, mod, _opts} -> Collection.has_dump_function?(mod) end)
          |> Enum.map(fn {name, mod, opts} -> {name, {opts[:name], mod}} end)

        embed_manys =
          @embed_manys
          |> Enum.filter(fn {_name, mod, _opts} -> Collection.has_dump_function?(mod) end)
          |> Enum.map(fn {name, mod, opts} -> {name, {opts[:name], mod}} end)

        def dump(nil), do: nil
        def dump(xs) when is_list(xs), do: Enum.map(xs, fn struct -> dump(struct) end)

        def dump(%{} = map) do
          map =
            unquote(embed_ones)
            |> Enum.map(fn {name, {_src_name, mod}} -> {name, mod.dump(Map.get(map, name))} end)
            |> Enum.reduce(map, fn {name, doc}, acc -> Map.put(acc, name, doc) end)

          map =
            unquote(embed_manys)
            |> Enum.map(fn {name, {_src_name, mod}} -> {name, mod.dump(Map.get(map, name))} end)
            |> Enum.reduce(map, fn {name, doc}, acc -> Map.put(acc, name, doc) end)

          map
          |> Map.drop(unquote(@derived))
          |> @before_dump_fun.()
          |> Collection.dump()
          |> Enum.into(%{}, fn {name, value} ->
            case unquote(attribute_names)[name] || unquote(embed_ones)[name] || unquote(embed_manys)[name] do
              {{src_name, _}, _mod} -> {src_name, value}
              {src_name, _} -> {src_name, value}
            end
          end)
        end
      end

    timestamps_function =
      quote unquote: false do
        def timestamps(nil), do: nil
        def timestamps(xs) when is_list(xs), do: Enum.map(xs, fn struct -> timestamps(struct) end)

        def timestamps(struct) do
          updated_at = @timestamps[:updated_at]
          Collection.timestamps(struct, updated_at, @attributes[updated_at])
        end

        def new_timestamps(struct) do
          inserted_at = @timestamps[:inserted_at]
          opts = @attributes[inserted_at]
          ts = opts[:default].()
          updated_at = @timestamps[:updated_at]

          struct
          |> Map.put(inserted_at, ts)
          |> Map.put(updated_at, ts)
        end
      end

    quote do
      unquote(prelude)
      unquote(postlude)
      unquote(new_function)
      unquote(load_function)
      unquote(dump_function)
      unquote(timestamps_function)
    end
  end

  @doc """
  Inserts name option for the attribute, embeds_one and embeds_many.
  """
  def add_name(mod, opts, name) do
    opts =
      case opts[:name] do
        nil -> Keyword.put(opts, :name, {name, to_string(name)})
        name when is_atom(name) -> Keyword.replace(opts, :name, {name, to_string(name)})
        name when is_binary(name) -> Keyword.replace(opts, :name, {String.to_atom(name), name})
        _ -> raise ArgumentError, "name must be an atom or a binary"
      end

    {src_name, _} = opts[:name]

    if name_exists?(mod, src_name),
      do: raise(ArgumentError, "attribute #{inspect(name)} has duplicate name option key\n\n    [name: #{inspect(src_name)}] already exist\n"),
      else: opts
  end

  defp name_exists?(mod, name) do
    name_exists?(mod, :attributes, name) || name_exists?(mod, :embed_ones, name) || name_exists?(mod, :embed_manys, name)
  end

  defp name_exists?(mod, :attributes, name) do
    mod
    |> Module.get_attribute(:attributes)
    |> Enum.any?(fn {_name, opts} -> elem(opts[:name], 0) == name end)
  end

  defp name_exists?(mod, embeds, name) do
    mod
    |> Module.get_attribute(embeds)
    |> Enum.any?(fn {_name, _mod, opts} -> elem(opts[:name], 0) == name end)
  end

  @doc """
  Inserts the specified `@id_generator` to the list of attributes. Calls `add_id/3`.
  """
  defmacro __id__(id_generator, name) do
    quote do
      Collection.add_id(__MODULE__, unquote(id_generator), unquote(name))
    end
  end

  @doc """
  Inserts the specified `@id_generator` to the list of attributes.
  """
  def add_id(_mod, _id_generator, nil) do
  end

  def add_id(_mod, {nil, _type, _fun}, _name) do
  end

  def add_id(mod, {id, type, fun}, _name) do
    Module.put_attribute(mod, :types, {id, type})
    Module.put_attribute(mod, :attributes, {id, default: fun, name: {:_id, "_id"}})
  end

  @doc """
  Inserts boilercode for the @type attribute.
  """
  defmacro __type__(types) do
    quote bind_quoted: [types: types] do
      @type t() :: %__MODULE__{unquote_splicing(types)}
    end
  end

  @doc """
  Returns true, if the Module has the `dump/1` function.
  """
  def has_dump_function?(mod) do
    Keyword.has_key?(mod.__info__(:functions), :dump)
  end

  @doc """
  Returns true, if the Module has the `load/1` function.
  """
  def has_load_function?(mod) do
    Keyword.has_key?(mod.__info__(:functions), :load)
  end

  @doc """
  Returns the default arguments for the struct. They are used to provide the
  default values in the `new/0` call.
  """
  def struct_args(args) when is_list(args) do
    Enum.map(args, fn {arg, func} -> struct_args(arg, func) end)
  end

  def struct_args(arg, func) do
    quote do
      {unquote(arg), unquote(func).()}
    end
  end

  @doc """
  Defines the `before_dump/1` function.
  """
  defmacro before_dump(fun) do
    quote do
      Module.put_attribute(__MODULE__, :before_dump_fun, unquote(fun))
    end
  end

  @doc """
  Defines the `after_load/1` function.
  """
  defmacro after_load(fun) do
    quote do
      Module.put_attribute(__MODULE__, :after_load_fun, unquote(fun))
    end
  end

  @doc """
  Adds the struct to the `embeds_one` list. Calls `__embeds_one__`
  """
  defmacro embeds_one(name, mod, opts \\ []) do
    quote do
      Collection.__embeds_one__(__MODULE__, unquote(name), unquote(mod), unquote(opts))
    end
  end

  @doc """
  Adds the struct to the `embeds_one` list.
  """
  def __embeds_one__(mod, name, target, opts) do
    Module.put_attribute(mod, :embed_ones, {name, target, add_name(mod, opts, name)})
  end

  @doc """
  Adds the struct to the `embeds_many` list. Calls `__embeds_many__`
  """
  defmacro embeds_many(name, mod, opts \\ []) do
    quote do
      type = unquote(Macro.escape({{:., [], [mod, :t]}, [], []}))
      Collection.__embeds_many__(__MODULE__, unquote(name), unquote(mod), type, unquote(opts))
    end
  end

  @doc """
  Adds the struct to the `embeds_many` list.
  """
  def __embeds_many__(mod, name, target, type, opts) do
    Module.put_attribute(mod, :types, {name, type})
    Module.put_attribute(mod, :embed_manys, {name, target, add_name(mod, opts, name)})
  end

  @doc """
  Adds the attribute to the attributes list. It call `__attribute__/4` function.
  """
  defmacro attribute(name, type, opts \\ []) do
    quote do
      Collection.__attribute__(__MODULE__, unquote(name), unquote(Macro.escape(type)), unquote(opts))
    end
  end

  @doc """
  Adds the attribute to the attributes list.
  """
  def __attribute__(mod, name, type, opts) do
    if opts[:derived],
      do: Module.put_attribute(mod, :derived, name),
      else: []

    Module.put_attribute(mod, :types, {name, type})
    Module.put_attribute(mod, :attributes, {name, add_name(mod, opts, name)})
  end

  @doc """
  Inserts src_name for the timestamp attribute
  """
  def timestamp(opts, name) when is_atom(name) do
    case Keyword.get(opts, name, {name, name}) do
      {name, src_name} -> {name, src_name}
      name -> {name, name}
    end
  end

  @doc """
  Defines the `timestamps/1` function.
  """
  defmacro timestamps(opts \\ []) do
    quote bind_quoted: [opts: opts] do
      {inserted_at, inserted_at_name} = timestamp(opts, :inserted_at)
      {updated_at, updated_at_name} = timestamp(opts, :updated_at)

      type = Keyword.get(opts, :type, DateTime)

      ops =
        opts
        |> Keyword.drop([:inserted_at, :updated_at, :type])
        |> Keyword.put_new(:default, &DateTime.utc_now/0)

      Module.put_attribute(__MODULE__, :timestamps, {:inserted_at, inserted_at})
      Module.put_attribute(__MODULE__, :timestamps, {:updated_at, updated_at})

      Collection.__attribute__(__MODULE__, inserted_at, Macro.escape(type), Keyword.put(ops, :name, inserted_at_name))
      Collection.__attribute__(__MODULE__, updated_at, Macro.escape(type), Keyword.put(ops, :name, updated_at_name))
    end
  end

  def timestamps(struct, nil, _default), do: struct

  def timestamps(struct, updated_at, opts) do
    Map.put(struct, updated_at, opts[:default].())
  end

  def dump(%{__struct__: _} = struct) do
    :maps.map(&dump/2, Map.from_struct(struct)) |> filter_nils()
  end

  def dump(map), do: :maps.map(&dump/2, map)
  def dump(_key, value), do: ensure_nested_map(value)

  defp ensure_nested_map(%{__struct__: Date} = data), do: data
  defp ensure_nested_map(%{__struct__: DateTime} = data), do: data
  defp ensure_nested_map(%{__struct__: NaiveDateTime} = data), do: data
  defp ensure_nested_map(%{__struct__: Time} = data), do: data
  defp ensure_nested_map(%{__struct__: BSON.ObjectId} = data), do: data
  defp ensure_nested_map(%{__struct__: BSON.Binary} = data), do: data
  defp ensure_nested_map(%{__struct__: BSON.Regex} = data), do: data
  defp ensure_nested_map(%{__struct__: BSON.JavaScript} = data), do: data
  defp ensure_nested_map(%{__struct__: BSON.Timestamp} = data), do: data
  defp ensure_nested_map(%{__struct__: BSON.LongNumber} = data), do: data

  defp ensure_nested_map(%{__struct__: _} = struct) do
    :maps.map(&dump/2, Map.from_struct(struct)) |> filter_nils()
  end

  defp ensure_nested_map(list) when is_list(list), do: Enum.map(list, &ensure_nested_map/1)

  defp ensure_nested_map(data), do: data

  def filter_nils(map) when is_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.into(%{})
  end

  def filter_nils(keyword) when is_list(keyword) do
    Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
  end
end
