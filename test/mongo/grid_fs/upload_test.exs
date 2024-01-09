defmodule Mongo.GridFs.UploadTest do
  use ExUnit.Case

  alias Mongo.GridFs.Bucket
  alias Mongo.GridFs.Upload

  setup_all do
    assert {:ok, pid} = Mongo.TestConnection.connect()
    {:ok, [pid: pid]}
  end

  def calc_checksum(path) do
    File.stream!(path, [], 2048)
    |> Enum.reduce(:crypto.hash_init(:sha256), fn line, acc -> :crypto.hash_update(acc, line) end)
    |> :crypto.hash_final()
    |> Base.encode16()
  end

  test "upload a jpeg file, check download, length and checksum", c do
    b = Bucket.new(c.pid, j: true, w: :majority)
    upload_stream = Upload.open_upload_stream(b, "test.jpg", nil)

    src_filename = "./test/data/test.jpg"
    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    dest_filename = "/tmp/my-test-file.jps"

    with {:ok, stream} <- Mongo.GridFs.Download.open_download_stream(b, file_id) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run()
    end

    assert true == File.exists?(dest_filename)

    %{size: dest_size} = File.stat!(dest_filename)
    %{size: src_size} = File.stat!(src_filename)
    assert dest_size == src_size

    assert calc_checksum(dest_filename) == calc_checksum(src_filename)
  end

  test "upload a text file, check download, length and checksum", c do
    b = Bucket.new(c.pid, j: true, w: :majority)
    upload_stream = Upload.open_upload_stream(b, "my-example-file.txt", meta: %{tag: "checked"})

    src_filename = "./test/data/test.txt"
    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    dest_filename = "/tmp/my-test-file.txt"

    with {:ok, stream} <- Mongo.GridFs.Download.open_download_stream(b, file_id) do
      stream
      |> Stream.into(File.stream!(dest_filename))
      |> Stream.run()
    end

    assert true == File.exists?(dest_filename)

    %{size: dest_size} = File.stat!(dest_filename)
    %{size: src_size} = File.stat!(src_filename)
    assert dest_size == src_size

    assert calc_checksum(dest_filename) == calc_checksum(src_filename)
  end

  test "upload a text file, check download, length, meta-data and checksum", c do
    src_filename = "./test/data/test.txt"
    bucket = Bucket.new(c.pid, j: true, w: :majority)
    chksum = calc_checksum(src_filename)
    upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt", %{tag: "checked", chk_sum: chksum})

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    file_id = upload_stream.id

    assert file_id != nil

    %{"metadata" => %{"tag" => "checked", "chk_sum" => x}} = Mongo.find_one(c.pid, Bucket.files_collection_name(bucket), %{_id: file_id})
    assert x == chksum
  end

  test "upload a text file with custom id, check download, length, meta-data and checksum", c do
    src_filename = "./test/data/test.txt"
    bucket = Bucket.new(c.pid, j: true, w: :majority)
    chksum = calc_checksum(src_filename)
    file_id = Mongo.object_id()

    upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt", %{tag: "checked", chk_sum: chksum}, file_id)

    File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()

    assert file_id == upload_stream.id

    %{"metadata" => %{"tag" => "checked", "chk_sum" => x}} = Mongo.find_one(c.pid, Bucket.files_collection_name(bucket), %{_id: file_id})
    assert x == chksum
  end

  @tag :mongo_4_2
  @tag :rs_required
  test "upload a text file, check download, length, meta-data and checksum transaction", c do
    src_filename = "./test/data/test.txt"
    chksum = calc_checksum(src_filename)
    bucket = Bucket.new(c.pid)

    {:ok, upload_stream} =
      Mongo.transaction(
        c.pid,
        fn ->
          upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt", %{tag: "checked", chk_sum: chksum})
          File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()
          {:ok, upload_stream}
        end,
        w: 1
      )

    file_id = upload_stream.id

    assert file_id != nil

    %{"metadata" => %{"tag" => "checked", "chk_sum" => x}} = Mongo.find_one(c.pid, Bucket.files_collection_name(bucket), %{_id: file_id})
    assert x == chksum
  end

  @tag :mongo_4_2
  @tag :rs_required
  test "upload a text file, check download, length, meta-data and checksum abort transaction", c do
    src_filename = "./test/data/test.txt"
    chksum = calc_checksum(src_filename)
    bucket = Bucket.new(c.pid)

    {:error, upload_stream} =
      Mongo.transaction(
        c.pid,
        fn ->
          upload_stream = Upload.open_upload_stream(bucket, "my-example-file.txt", %{tag: "checked", chk_sum: chksum})
          File.stream!(src_filename, [], 512) |> Stream.into(upload_stream) |> Stream.run()
          {:error, upload_stream}
        end,
        w: 1
      )

    file_id = upload_stream.id

    assert file_id != nil

    assert nil == Mongo.find_one(c.pid, Bucket.files_collection_name(Bucket.new(c.pid)), %{_id: file_id})
  end
end
