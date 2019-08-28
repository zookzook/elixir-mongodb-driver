defmodule BSON.Decimal128 do
  use Bitwise

  @signed_bit_mask 1 <<< 63
  @combination_mask 0x1f
  @combintation_infinity 30
  @combintation_nan 31
  @exponent_mask 0x3fff
  @exponent_bias 6176
  @max_exponent 6111
  @min_exponent -6176

  def decode(<<_::little-64, high::little-64>> = bits) do
    is_negative = (high &&& @signed_bit_mask) == (@signed_bit_mask)
    combination = (high >>> 58 &&& @combination_mask)
    two_highest_bits_set = combination >>> 3 == 3
    is_infinity = two_highest_bits_set && combination == @combintation_infinity
    is_nan = two_highest_bits_set && combination == @combintation_nan

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

  def encode(%Decimal{sign: sign, coef: significand, exp: exponent}) when exponent >= @min_exponent and exponent <= @max_exponent do
    biasedExponent = exponent + @exponent_bias
    low  = significand &&& 0xffffffff
    high = (significand >>> 64) &&& 0xffffffff
    high = bor(high, biasedExponent <<< 49)
    high = case sign do
      1 -> high
      _ -> bor(high, @signed_bit_mask)
    end

    <<low::little-64, high::little-64>>
  end

  def encode(%Decimal{sign: sign, coef: significand, exp: exponent}) do
    message = "Exponent is out of range for Decimal128 encoding"
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

  defp value(%{is_nan: true}, _, _) do
    %Decimal{coef: :qNaN}
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