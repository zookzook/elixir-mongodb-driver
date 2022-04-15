defmodule Mongo.InsertOneResult do
  @moduledoc """
  The successful result struct of `Mongo.insert_one/4`. Its fields are:

    * `:inserted_id` - The id of the inserted document
  """

  @type t :: %__MODULE__{
          inserted_id: nil | BSON.ObjectId.t()
        }

  defstruct acknowledged: true, inserted_id: nil
end

defmodule Mongo.InsertManyResult do
  @moduledoc """
  The successful result struct of `Mongo.insert_many/4`. Its fields are:

    * `:inserted_ids` - The ids of the inserted documents indexed by their order
  """

  @type t :: %__MODULE__{
          inserted_ids: %{non_neg_integer => BSON.ObjectId.t()}
        }

  defstruct acknowledged: true, inserted_ids: nil
end

defmodule Mongo.DeleteResult do
  @moduledoc """
  The successful result struct of `Mongo.delete_one/4` and `Mongo.delete_many/4`.
  Its fields are:

    * `:deleted_count` - Number of deleted documents
    * `:acknowledged` - Write-concern
  """

  @type t :: %__MODULE__{
          acknowledged: boolean,
          deleted_count: non_neg_integer
        }

  defstruct acknowledged: true, deleted_count: 0
end

defmodule Mongo.UpdateResult do
  @moduledoc """
  The successful result struct of `Mongo.update_one/5`, `Mongo.update_many/5`
  and `Mongo.replace_one/5`. Its fields are:

    * `:matched_count` - Number of matched documents
    * `:modified_count` - Number of modified documents
    * `:upserted_ids` - If the operation was an upsert, the upserted ids
  """

  @type t :: %__MODULE__{
          acknowledged: boolean,
          matched_count: non_neg_integer,
          modified_count: non_neg_integer,
          upserted_ids: list(BSON.ObjectId.t())
        }

  defstruct acknowledged: true, matched_count: 0, modified_count: 0, upserted_ids: []
end

defmodule Mongo.BulkWriteResult do
  @moduledoc """
  The successful result struct of `Mongo.BulkWrite.write`. Its fields are:

    * `:acknowledged` - Write-concern
    * `:matched_count` - Number of matched documents
    * `:modified_count` - Number of modified documents
    * `:inserted_count` - Number of inserted documents
    * `:deleted_count` - Number of deleted documents
    * `:upserted_count` - Number of upserted documents
    * `:upserted_ids` - If the operation was an upsert, the upserted ids
    * `:inserted_ids` - If the operation was an insert, the inserted ids
    * `:errors` - If the operation results in an error, the error is collected

  """

  @type t :: %__MODULE__{
          acknowledged: boolean,
          matched_count: non_neg_integer,
          modified_count: non_neg_integer,
          inserted_count: non_neg_integer,
          deleted_count: non_neg_integer,
          upserted_count: non_neg_integer,
          upserted_ids: list(BSON.ObjectId.t()),
          inserted_ids: list(BSON.ObjectId.t()),
          errors: list(map)
        }

  alias Mongo.BulkWriteResult

  defstruct acknowledged: true,
            matched_count: 0,
            modified_count: 0,
            inserted_count: 0,
            deleted_count: 0,
            upserted_count: 0,
            inserted_ids: [],
            upserted_ids: [],
            errors: []

  def insert_result(count, ids, errors) do
    ids = Enum.reduce(errors, ids, fn error, ids -> filter_ids(ids, error) end)
    %BulkWriteResult{inserted_count: count, inserted_ids: ids, errors: errors}
  end

  defp filter_ids(ids, %{"code" => 11_000, "index" => index}) do
    Enum.take(ids, index)
  end

  defp filter_ids(ids, _other) do
    ids
  end

  def update_result(matched_count, modified_count, upserted_count, ids, errors) do
    %BulkWriteResult{matched_count: matched_count, modified_count: modified_count, upserted_count: upserted_count, upserted_ids: ids, errors: errors}
  end

  def delete_result(count, errors) do
    %BulkWriteResult{deleted_count: count, errors: errors}
  end

  def error(error) do
    %BulkWriteResult{errors: [error]}
  end

  def empty() do
    %BulkWriteResult{}
  end

  def add(%BulkWriteResult{} = src, %BulkWriteResult{} = dest) do
    %BulkWriteResult{
      acknowledged: src.acknowledged,
      matched_count: src.matched_count + dest.matched_count,
      modified_count: src.modified_count + dest.modified_count,
      inserted_count: src.inserted_count + dest.inserted_count,
      deleted_count: src.deleted_count + dest.deleted_count,
      upserted_count: src.upserted_count + dest.upserted_count,
      inserted_ids: src.inserted_ids ++ dest.inserted_ids,
      upserted_ids: src.upserted_ids ++ dest.upserted_ids,
      errors: src.errors ++ dest.errors
    }
  end

  def reduce(results, acc) do
    Enum.reduce(results, acc, fn x, acc -> BulkWriteResult.add(acc, x) end)
  end

  def reduce(results) do
    reduce(results, %BulkWriteResult{})
  end
end
