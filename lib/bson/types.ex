defmodule BSON.Binary do
  @moduledoc """
  Represents BSON binary type
  """

  @type t :: %__MODULE__{
          binary: binary,
          subtype: :generic | :function | :binary_old | :uuid_old | :uuid | :md5 | 0x80..0xFF
        }
  defstruct binary: nil, subtype: :generic

  defimpl String.Chars, for: BSON.Binary do
    def to_string(%BSON.Binary{binary: value, subtype: subtype}) when subtype in [:uuid, :uuid_old] do
      p1 = binary_part(value, 0, 4)
      p2 = binary_part(value, 4, 2)
      p3 = binary_part(value, 6, 2)
      p4 = binary_part(value, 8, 2)
      p5 = binary_part(value, 10, 6)
      "#{Base.encode16(p1, case: :lower)}-#{Base.encode16(p2, case: :lower)}-#{Base.encode16(p3, case: :lower)}-#{Base.encode16(p4, case: :lower)}-#{Base.encode16(p5, case: :lower)}"
    end

    def to_string(%BSON.Binary{binary: value}) do
      Base.encode16(value, case: :lower)
    end
  end

  defimpl Inspect do
    def inspect(%BSON.Binary{subtype: :generic} = binary, _opts) do
      "#BSON.Binary<#{to_string(binary)}>"
    end

    def inspect(%BSON.Binary{subtype: :uuid} = binary, _opts) do
      "#BSON.UUID<#{to_string(binary)}>"
    end

    def inspect(%BSON.Binary{subtype: :uuid_old} = binary, _opts) do
      "#BSON.LUUID<#{to_string(binary)}>"
    end

    def inspect(%BSON.Binary{subtype: subtype} = binary, _opts) do
      "#BSON.Binary<#{to_string(binary)}, #{subtype}>"
    end
  end
end

defmodule BSON.ObjectId do
  @moduledoc """
  Represents BSON ObjectId type
  """
  defstruct [:value]

  @type t :: %__MODULE__{value: <<_::96>>}

  @doc """
  Creates a new ObjectId from the consisting parts
  """
  def new(machine_id, proc_id, secs, counter) do
    value = <<secs::unsigned-big-32, machine_id::unsigned-big-24, proc_id::unsigned-big-16, counter::unsigned-big-24>>
    %BSON.ObjectId{value: value}
  end

  @doc """
  Converts string representation of ObjectId to a BSON.ObjectId struct
  """
  def decode!(<<c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23>>) do
    <<d(c0)::4, d(c1)::4, d(c2)::4, d(c3)::4, d(c4)::4, d(c5)::4, d(c6)::4, d(c7)::4, d(c8)::4, d(c9)::4, d(c10)::4, d(c11)::4, d(c12)::4, d(c13)::4, d(c14)::4, d(c15)::4, d(c16)::4, d(c17)::4, d(c18)::4, d(c19)::4, d(c20)::4, d(c21)::4, d(c22)::4,
      d(c23)::4>>
  catch
    :throw, :error -> raise ArgumentError
  else
    value -> %BSON.ObjectId{value: value}
  end

  @doc """
  Converts string representation of ObjectId to a BSON.ObjectId struct

  ## Examples
      {:ok, id} <- BSON.ObjectId.decode(bson_id)
  """
  def decode(id) do
    try do
      {:ok, decode!(id)}
    rescue
      _ -> :error
    end
  end

  @doc """
  Converts BSON.ObjectId struct to a string representation
  """
  def encode!(%BSON.ObjectId{value: value}), do: do_encode(value)

  defp do_encode(<<l0::4, h0::4, l1::4, h1::4, l2::4, h2::4, l3::4, h3::4, l4::4, h4::4, l5::4, h5::4, l6::4, h6::4, l7::4, h7::4, l8::4, h8::4, l9::4, h9::4, l10::4, h10::4, l11::4, h11::4>>) do
    <<e(l0), e(h0), e(l1), e(h1), e(l2), e(h2), e(l3), e(h3), e(l4), e(h4), e(l5), e(h5), e(l6), e(h6), e(l7), e(h7), e(l8), e(h8), e(l9), e(h9), e(l10), e(h10), e(l11), e(h11)>>
  catch
    :throw, :error ->
      raise ArgumentError
  else
    value ->
      value
  end

  @doc """
  Converts BSON.ObjectId struct to a string representation

  ## Examples
      {:ok, bson_id} <- BSON.ObjectId.encode(id)
  """
  def encode(object_id) do
    try do
      {:ok, encode!(object_id)}
    rescue
      _ -> :error
    end
  end

  @compile {:inline, :d, 1}
  @compile {:inline, :e, 1}

  @chars Enum.concat(?0..?9, ?a..?f)

  for {char, int} <- Enum.with_index(@chars) do
    defp d(unquote(char)), do: unquote(int)
    defp e(unquote(int)), do: unquote(char)
  end

  for {char, int} <- Enum.with_index(?A..?F) do
    defp d(unquote(char)), do: unquote(int)
  end

  defp d(_), do: throw(:error)

  defp e(_), do: throw(:error)

  defimpl Inspect do
    def inspect(objectid, _opts) do
      encoded = BSON.ObjectId.encode!(objectid)
      "#BSON.ObjectId<#{encoded}>"
    end
  end

  defimpl String.Chars do
    def to_string(id), do: BSON.ObjectId.encode!(id)
  end
end

defmodule BSON.Regex do
  @moduledoc """
  Represents BSON Regex type
  """

  @type t :: %__MODULE__{pattern: binary, options: binary}
  defstruct pattern: "", options: ""

  defimpl Inspect do
    def inspect(%BSON.Regex{pattern: pattern, options: nil}, _opts) do
      "#BSON.Regex<#{inspect(pattern)}>"
    end

    def inspect(%BSON.Regex{pattern: pattern, options: options}, _opts) do
      "#BSON.Regex<#{inspect(pattern)}, #{inspect(options)}>"
    end
  end
end

defmodule BSON.JavaScript do
  @moduledoc """
  Represents BSON JavaScript (with and without scope) types
  """

  @type t :: %__MODULE__{code: binary, scope: %{binary => BSON.t()}}
  defstruct [:code, :scope]

  defimpl Inspect do
    def inspect(%BSON.JavaScript{code: code, scope: nil}, _opts) do
      "#BSON.JavaScript<#{inspect(code)}>"
    end

    def inspect(%BSON.JavaScript{code: code, scope: scope}, _opts) do
      "#BSON.JavaScript<#{inspect(code)}, #{inspect(scope)}>"
    end
  end
end

defmodule BSON.Timestamp do
  @moduledoc """
  Represents BSON Timestamp type
  """

  @type t :: %__MODULE__{value: integer, ordinal: integer}
  defstruct [:value, :ordinal]

  def is_after(this, that) do
    this.value > that.value or (this.value == that.value and this.ordinal > that.ordinal)
  end

  def is_before(this, that) do
    this.value < that.value or (this.value == that.value and this.ordinal < that.ordinal)
  end

  defimpl Inspect do
    def inspect(%BSON.Timestamp{value: value, ordinal: ordinal}, _opts) do
      "#BSON.Timestamp<#{value}:#{ordinal}>"
    end
  end
end

defmodule BSON.LongNumber do
  @moduledoc """
  Represents BSON long type
  """

  @type t :: %__MODULE__{value: integer}

  defstruct value: 0

  defimpl Inspect do
    def inspect(%BSON.LongNumber{value: value}, _opts) do
      "#BSON.LongNumber<#{value}>"
    end
  end
end
