defmodule BSON.Decimal128 do

  @moduledoc """
    see https://en.wikipedia.org/wiki/Decimal128_floating-point_format
  """

  use Bitwise

  @signed_bit_mask 1 <<< 63
  @combination_mask 0x1f
  @combintation_infinity 30
  @combintation_nan 31
  @exponent_mask 0x3fff
  @exponent_bias 6176
  @max_exponent 6111
  @min_exponent -6176
  @s_nan_mask  0x1 <<< 57
  @significand_mask ((0x1 <<< 49)-1)
  @low_mask 0xffffffffffffffff

  def decode(<<_::little-64, high::little-64>> = bits) do
    is_negative = (high &&& @signed_bit_mask) == (@signed_bit_mask)
    combination = (high >>> 58 &&& @combination_mask)
    two_highest_bits_set = combination >>> 3 == 3
    is_infinity = two_highest_bits_set && combination == @combintation_infinity
    is_nan = case {(two_highest_bits_set && combination) == @combintation_nan, (high &&& @s_nan_mask) == @s_nan_mask} do
      {true, true}  -> :sNan
      {true, false} -> :qNan
                  _ -> false
    end

    exponent = exponent(high, two_highest_bits_set)

    value(
      %{is_negative: is_negative,
        is_infinity: is_infinity,
        is_nan: is_nan,
        two_highest_bits_set: two_highest_bits_set},
      coef(bits),
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
    high = 0x3e <<< 58
    <<low::little-64, high::little-64>>
  end
  def encode(%Decimal{coef: :inf}) do
    low = 0
    high = 0x1e <<< 58
    <<low::little-64, high::little-64>>
  end
  def encode(%Decimal{coef: :qNaN}) do
    low = 0
    high = 0x1f <<< 58
    <<low::little-64, high::little-64>>
  end
  def encode(%Decimal{coef: :sNaN}) do
    low = 0
    high = 0x3f <<< 57
    <<low::little-64, high::little-64>>
  end
  def encode(%Decimal{sign: sign, coef: significand, exp: exponent}) when exponent >= @min_exponent and exponent <= @max_exponent do
    biasedExponent = exponent + @exponent_bias
    low  = significand &&& @low_mask
    high = (significand >>> 64) &&& @significand_mask ## mask max significand
    high = bor(high, biasedExponent <<< 49)
    high = case sign do
      1 -> high
      _ -> bor(high, @signed_bit_mask)
    end

    <<low::little-64, high::little-64>>
  end
  def encode(%Decimal{exp: exponent}) do
    message = "Exponent is out of range for Decimal128 encoding, #{exponent}"
    raise ArgumentError, message
  end

  defp exponent(high, _two_highest_bits_set = true) do
    biased_exponent = (high >>> 47) &&& @exponent_mask
    biased_exponent - @exponent_bias
  end
  defp exponent(high, _two_highest_bits_not_set) do
    biased_exponent = (high >>> 49) &&& @exponent_mask
    biased_exponent - @exponent_bias
  end

  defp value(%{is_negative: true, is_infinity: true}, _, _) do
    %Decimal{sign: -1, coef: :inf}
  end
  defp value(%{is_negative: false, is_infinity: true}, _, _) do
    %Decimal{coef: :inf}
  end
  defp value(%{is_nan: :qNan}, _, _) do
    %Decimal{coef: :qNaN}
  end
  defp value(%{is_nan: :sNan}, _, _) do
    %Decimal{coef: :sNaN}
  end
  defp value(%{two_highest_bits_set: true}, _, _) do
    %Decimal{sign: 0, coef: 0, exp: 0}
  end
  defp value(%{is_negative: true}, coef, exponent) do
    %Decimal{sign: -1, coef: coef, exp: exponent}
  end
  defp value(_, coef, exponent) do
    %Decimal{coef: coef, exp: exponent}
  end

  defp coef(<<low::little-64, high::little-64>>) do
    bor((high &&& 0x1ffffffffffff) <<< 64, low)
  end
end