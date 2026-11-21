defmodule BSON.Decimal128 do
  @moduledoc """
    see https://en.wikipedia.org/wiki/Decimal128_floating-point_format
  """

  import Bitwise

  @signed_bit_mask 1 <<< 63
  @combination_mask 0x1F
  @combintation_infinity 30
  @combintation_nan 31
  @exponent_mask 0x3FFF
  @exponent_bias 6176
  @max_exponent 6111
  @min_exponent -6176
  @significand_mask (0x1 <<< 49) - 1
  @max_significand 9_999_999_999_999_999_999_999_999_999_999_999
  @low_mask 0xFFFFFFFFFFFFFFFF

  def decode(<<_::little-64, high::little-64>> = bits) do
    is_negative = (high &&& @signed_bit_mask) == @signed_bit_mask
    combination = high >>> 58 &&& @combination_mask
    two_highest_bits_set = combination >>> 3 == 3
    is_infinity = two_highest_bits_set && combination == @combintation_infinity
    is_nan = two_highest_bits_set && combination == @combintation_nan
    exponent = exponent(high, two_highest_bits_set)

    value(
      %{is_negative: is_negative, is_infinity: is_infinity, is_nan: is_nan},
      coef(bits, two_highest_bits_set),
      exponent
    )
  end

  @doc """
  s 11110 xx...x    ±infinity
  s 11111 0x...x    a quiet NaN
  s 11111 1x...x    a signalling NaN
  """
  def encode(%Decimal{sign: -1, coef: :inf}) do
    low = 0
    high = 0x3E <<< 58
    <<low::little-64, high::little-64>>
  end

  def encode(%Decimal{coef: :inf}) do
    low = 0
    high = 0x1E <<< 58
    <<low::little-64, high::little-64>>
  end

  def encode(%Decimal{coef: :NaN}) do
    low = 0
    high = 0x1F <<< 58
    <<low::little-64, high::little-64>>
  end

  def encode(%Decimal{sign: sign, coef: significand, exp: exponent} = decimal) do
    case clamp(significand, exponent) do
      {significand, _exponent} when significand > @max_significand ->
        raise ArgumentError, "Coefficient has more than 34 digits for Decimal128 encoding, #{Decimal.to_string(decimal)}"

      {significand, exponent} when exponent >= @min_exponent and exponent <= @max_exponent ->
        encode_finite(sign, significand, exponent)

      {_significand, exponent} ->
        raise ArgumentError, "Exponent is out of range for Decimal128 encoding, #{exponent}"
    end
  end

  # Brings the exponent into range or the coefficient down to 34 digits
  # without changing the value, by adding or removing trailing zeros.
  defp clamp(0, exponent), do: {0, exponent |> max(@min_exponent) |> min(@max_exponent)}

  defp clamp(significand, exponent) when exponent > @max_exponent and significand * 10 <= @max_significand do
    clamp(significand * 10, exponent - 1)
  end

  defp clamp(significand, exponent) when (exponent < @min_exponent or significand > @max_significand) and rem(significand, 10) == 0 and exponent < @max_exponent do
    clamp(div(significand, 10), exponent + 1)
  end

  defp clamp(significand, exponent), do: {significand, exponent}

  defp encode_finite(sign, significand, exponent) do
    biased_exponent = exponent + @exponent_bias
    low = significand &&& @low_mask
    ## mask max significand
    high = significand >>> 64 &&& @significand_mask
    high = bor(high, biased_exponent <<< 49)

    high =
      case sign do
        1 -> high
        _ -> bor(high, @signed_bit_mask)
      end

    <<low::little-64, high::little-64>>
  end

  defp exponent(high, true) do
    biased_exponent = high >>> 47 &&& @exponent_mask
    biased_exponent - @exponent_bias
  end

  defp exponent(high, _two_highest_bits_not_set) do
    biased_exponent = high >>> 49 &&& @exponent_mask
    biased_exponent - @exponent_bias
  end

  defp value(%{is_negative: true, is_infinity: true}, _, _) do
    %Decimal{sign: -1, coef: :inf}
  end

  defp value(%{is_negative: false, is_infinity: true}, _, _) do
    %Decimal{coef: :inf}
  end

  defp value(%{is_nan: true}, _, _) do
    %Decimal{coef: :NaN}
  end

  defp value(%{is_negative: true}, coef, exponent) do
    %Decimal{sign: -1, coef: coef, exp: exponent}
  end

  defp value(_, coef, exponent) do
    %Decimal{coef: coef, exp: exponent}
  end

  # A significand in the "11" form is always larger than the maximum,
  # and a larger significand is not canonical: both are read as zero.
  defp coef(_bits, true = _two_highest_bits_set), do: 0

  defp coef(<<low::little-64, high::little-64>>, false) do
    case bor((high &&& 0x1FFFFFFFFFFFF) <<< 64, low) do
      significand when significand > @max_significand -> 0
      significand -> significand
    end
  end
end
