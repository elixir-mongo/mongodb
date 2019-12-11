defmodule BSON.Decimal128 do
  use Bitwise

  @signed_bit_mask 1 <<< 63
  @combination_mask 0x1F
  @combintation_infinity 30
  @combintation_nan 31
  @exponent_mask 0x3FFF
  @exponent_bias 6176

  @nan_mask 0x7C00000000000000
  @inf_mask 0x7800000000000000

  def decode(<<_::little-64, high::little-64>> = bits) do
    is_negative = (high &&& @signed_bit_mask) == @signed_bit_mask
    combination = high >>> 58 &&& @combination_mask
    two_highest_bits_set = combination >>> 3 == 3
    is_infinity = two_highest_bits_set && combination == @combintation_infinity
    is_nan = two_highest_bits_set && combination == @combintation_nan

    exponent = exponent(high, two_highest_bits_set)

    value(
      %{
        is_negative: is_negative,
        is_infinity: is_infinity,
        is_nan: is_nan,
        two_highest_bits_set: two_highest_bits_set
      },
      coef(bits),
      exponent
    )
  end

  def encode(%Decimal{coef: :qNaN, sign: sign}) do
    low = 0
    high = set_signed(@nan_mask, sign)

    to_binary(low, high)
  end

  def encode(%Decimal{coef: :inf, sign: sign}) do
    low = 0
    high = set_signed(@inf_mask, sign)

    to_binary(low, high)
  end

  def encode(%Decimal{sign: sign, coef: coef, exp: exp}) do
    low = coef &&& (1 <<< 64) - 1

    high =
      coef >>> 64
      |> set_exponent(exp)
      |> set_signed(sign)

    to_binary(low, high)
  end

  defp to_binary(low, high) do
    to_unsigned_binary(low) <> to_unsigned_binary(high)
  end

  defp set_exponent(high, exp) do
    two_highest_bits_set = high >>> 49 == 1
    set_exponent(high, exp, two_highest_bits_set)
  end

  defp set_exponent(high, exp, false = _two_highest_bits_set) do
    biased_exponent = exp + @exponent_bias
    bor(high, biased_exponent <<< 49)
  end

  defp set_exponent(high, exp, true = _two_highest_bits_set) do
    biased_exponent = exp + @exponent_bias
    high = high &&& 0x7FFFFFFFFFFF
    high = bor(high, 3 <<< 61)
    shifted_exponent = biased_exponent &&& @exponent_mask <<< 47
    bor(high, shifted_exponent)
  end

  defp set_signed(high, 1) do
    high
  end

  defp set_signed(high, -1) do
    high ||| @signed_bit_mask
  end

  defp to_unsigned_binary(value) do
    pad_trailing(:binary.encode_unsigned(value, :little), 8, 0)
  end

  defp pad_trailing(binary, len, _byte) when byte_size(binary) >= len do
    binary
  end

  defp pad_trailing(binary, len, byte) do
    binary <> :binary.copy(<<byte>>, len - byte_size(binary))
  end

  defp exponent(high, _two_highest_bits_set = true) do
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
    bor((high &&& 0x1FFFFFFFFFFFF) <<< 64, low)
  end
end
