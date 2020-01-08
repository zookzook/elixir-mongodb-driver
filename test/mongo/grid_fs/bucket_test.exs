defmodule Mongo.GridFs.BucketTest do
  use ExUnit.Case, async: false

  alias Mongo.GridFs.Bucket
  alias Mongo.GridFs.Upload
  alias BSON.ObjectId
  alias Mongo.Session

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    {:ok, [pid: pid]}
  end

  test "check if the name can be overridden", c do
    new_name = "my_fs"
    %Bucket{name: fs} = Bucket.new(c.pid, name: "my_fs")
    assert fs == new_name
  end

  test "check if the chunk_size can be overridden", c do
    new_chunk_size = 30 * 1024
    %Bucket{chunk_size: chunk_size} = Bucket.new(c.pid, chunk_size: new_chunk_size)
    assert chunk_size == new_chunk_size
  end

  test "delete a file", c do
    bucket        = Bucket.new(c.pid)
    upload_stream = Upload.open_upload_stream(bucket, "my-file-to-delete.txt")
    src_filename  = "./test/data/test.txt"

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    file = Bucket.find_one(bucket, file_id)
    assert file != nil

    Bucket.delete(bucket, file_id)

    file = Bucket.find_one(bucket, file_id)
    assert file == nil

    chunk = Mongo.find_one(c.pid, Bucket.chunks_collection_name(bucket), %{files_id: file_id})
    assert chunk == nil
  end

  test "delete a file with ID a string", c do
    bucket        = Bucket.new(c.pid)
    upload_stream = Upload.open_upload_stream(bucket, "my-file-to-delete.txt")
    src_filename  = "./test/data/test.txt"

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    file = Bucket.find_one(bucket, file_id)
    assert file != nil

    Bucket.delete(bucket, ObjectId.encode!(file_id))

    file = Bucket.find_one(bucket, file_id)
    assert file == nil

    chunk = Mongo.find_one(c.pid, Bucket.chunks_collection_name(bucket), %{files_id: file_id})
    assert chunk == nil
  end

  test "rename a file", c do

    bucket        = Bucket.new(c.pid)
    new_filename  = "my-new-filename.txt"
    upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt")
    src_filename  = "./test/data/test.txt"

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    file = Bucket.find_one(bucket, file_id)
    assert file != nil

    Bucket.rename(bucket, file_id, new_filename)

    new_file = Bucket.find_one(bucket, file_id)

    assert new_filename == new_file["filename"]
  end

  test "drop bucket", c do

    bucket        = Bucket.new(c.pid, name: "killme")
    upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt")
    src_filename  = "./test/data/test.txt"

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id
    file = Bucket.find_one(bucket, file_id)
    assert file != nil

    Bucket.drop(bucket)

    file = Mongo.find_one(c.pid, Bucket.files_collection_name(bucket), %{_id: file_id})
    assert file == nil

    file = Mongo.find_one(c.pid, Bucket.chunks_collection_name(bucket), %{file_id: file_id})
    assert file == nil

  end

  test "check find and find_one", c do

    bucket        = Bucket.new(c.pid, name: "killme")
    upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt")
    src_filename  = "./test/data/test.txt"

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id
    [file] = Bucket.find(bucket, [_id: file_id]) |> Enum.to_list()
    assert file != nil
    file = Bucket.find_one(bucket, ObjectId.encode!(file_id))
    assert file != nil

    Bucket.drop(bucket)
  end

  @tag :mongo_4_2
  test "explicit sessions", c do

    top = c.pid
    {:ok, session} = Session.start_session(top, :write, [])
    assert :ok = Session.start_transaction(session)

    bucket = Bucket.new(top, name: "sessions") |> Bucket.add_session(session: session)

    upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt")
    src_filename  = "./test/data/test.txt"

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id
    file = Bucket.find_one(bucket, file_id)
    assert file != nil

    assert :ok = Session.abort_transaction(session)
    assert :ok == Session.end_session(top, session)

    bucket = Bucket.new(top, name: "sessions")
    file = Bucket.find_one(bucket, file_id)
    assert file == nil

  end

  test "check if indices are created", c do

    top = c.pid
    _bucket = Bucket.new(top, name: "index_test")

    assert Mongo.list_index_names(top, "index_test.files") |> Enum.any?(fn name -> name == "filename_1_uploadDate_1" end)
    assert Mongo.list_index_names(top, "index_test.chunks") |> Enum.any?(fn name -> name == "files_id_1_n_1" end)

    assert :ok == Mongo.drop_index(top, "index_test.files", "filename_1_uploadDate_1")
    assert :ok == Mongo.drop_index(top, "index_test.chunks", "files_id_1_n_1")

    assert false == Mongo.list_index_names(top, "index_test.files") |> Enum.any?(fn name -> name == "filename_1_uploadDate_1" end)
    assert false == Mongo.list_index_names(top, "index_test.chunks") |> Enum.any?(fn name -> name == "files_id_1_n_1" end)

    _bucket = Bucket.new(top, name: "index_test")

    assert Mongo.list_index_names(top, "index_test.files") |> Enum.any?(fn name -> name == "filename_1_uploadDate_1" end)
    assert Mongo.list_index_names(top, "index_test.chunks") |> Enum.any?(fn name -> name == "files_id_1_n_1" end)
  end

end
