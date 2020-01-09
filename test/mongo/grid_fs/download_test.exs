defmodule Mongo.GridFs.DownloadTest do
  use ExUnit.Case

  alias Mongo.GridFs.Bucket
  alias Mongo.GridFs.Upload
  alias Mongo.GridFs.Download
  alias BSON.ObjectId

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect
    bucket = Bucket.new(pid)

    upload_stream = Upload.open_upload_stream(bucket, "test.jpg", nil)
    src_filename = "./test/data/test.jpg"
    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()
    file_id = upload_stream.id
    assert file_id != nil

    {:ok, [pid: pid, bucket: bucket, id: file_id]}
  end

  test "open_download_stream - binary", c do

    dest_filename = "/tmp/my-test-file.jps"
    File.rm(dest_filename)

    with {:ok, stream} <- Download.open_download_stream(c.bucket, ObjectId.encode!(c.id)) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
    end

    assert true == File.exists?(dest_filename)

  end

  test "open_download_stream - object id", c do

    dest_filename = "/tmp/my-test-file.jps"
    File.rm(dest_filename)

    with {:ok, stream} <- Download.open_download_stream(c.bucket, c.id) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
    end

    assert true == File.exists?(dest_filename)

  end

  test "open_download_stream - map ", c do

    assert c.id != nil
    file = Download.find_one_file(c.bucket, c.id)

    dest_filename = "/tmp/my-test-file.jps"
    File.rm(dest_filename)

    with {:ok, stream} <- Download.open_download_stream(c.bucket, file) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
    end

    assert true == File.exists?(dest_filename)

  end

  test "find_and_stream", c do

    dest_filename = "/tmp/my-test-file.jps"
    File.rm(dest_filename)

    with {{:ok, stream}, file_info} <- Download.find_and_stream(c.bucket, c.id) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
      assert file_info["filename"] == "test.jpg"
    end

    assert true == File.exists?(dest_filename)

    File.rm(dest_filename)

    with {{:ok, stream}, file_info} <- Download.find_and_stream(c.bucket, ObjectId.encode!(c.id)) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
      assert file_info["filename"] == "test.jpg"
    end

    assert true == File.exists?(dest_filename)

  end

  test "find_one_file - filename ", c do

    assert c.id != nil
    file = Download.find_one_file(c.bucket, "test.jpg")

    dest_filename = "/tmp/my-test-file.jps"
    File.rm(dest_filename)

    with {:ok, stream} <- Download.open_download_stream(c.bucket, file) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run
    end

    assert true == File.exists?(dest_filename)

  end

end
