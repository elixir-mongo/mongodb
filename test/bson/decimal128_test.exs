defmodule BSON.Decimal128Test do
  use ExUnit.Case, async: true

  @nan_binaries <<00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 124>>
  @inf_binaries <<00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 120>>
  @neg_inf_binaries <<00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 248>>
  @binaries_0 <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 48>>
  @binaries_0_neg_expo <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 122, 43>>
  @binaries_neg_0_0 <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 176>>
  @binaries_1_e_3 <<1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 70, 48>>
  @binaries_0_001234 <<210, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 52, 48>>
  @binaries_0_00123400000 <<64, 239, 90, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 48>>
  @binaries_0_1234567890123456789012345678901234 <<242, 175, 150, 126, 208, 92, 130, 222, 50, 151,
                                                   255, 111, 222, 60, 252, 47>>
  @binaries_regular_largest <<242, 175, 150, 126, 208, 92, 130, 222, 50, 151, 255, 111, 222, 60,
                              64, 48>>
  @binaries_scientific_tiniest <<255, 255, 255, 255, 99, 142, 141, 55, 192, 135, 173, 190, 9, 237,
                                 1, 0>>
  @binaries_scientific_tiny <<1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
  @binaries_neg_tiny <<1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128>>

  @tag :mongo_3_4
  test "BSON.Decimal128.decode/1" do
    assert_decimal(@nan_binaries, %Decimal{coef: :qNaN})
    assert_decimal(@inf_binaries, %Decimal{coef: :inf})
    assert_decimal(@neg_inf_binaries, %Decimal{sign: -1, coef: :inf})
    assert_decimal(@binaries_0, %Decimal{coef: 0})
    assert_decimal(@binaries_0_neg_expo, %Decimal{coef: 0, exp: -611})
    assert_decimal(@binaries_neg_0_0, %Decimal{sign: -1, coef: 0, exp: -1})
    assert_decimal(@binaries_1_e_3, %Decimal{coef: 1, exp: 3})
    assert_decimal(@binaries_0_001234, %Decimal{coef: 1234, exp: -6})
    assert_decimal(@binaries_0_00123400000, %Decimal{coef: 123_400_000, exp: -11})

    assert_decimal(@binaries_0_1234567890123456789012345678901234, %Decimal{
      coef: 1_234_567_890_123_456_789_012_345_678_901_234,
      exp: -34
    })

    assert_decimal(@binaries_regular_largest, %Decimal{
      coef: 1_234_567_890_123_456_789_012_345_678_901_234,
      exp: 0
    })

    assert_decimal(@binaries_scientific_tiniest, %Decimal{
      coef: 9_999_999_999_999_999_999_999_999_999_999_999,
      exp: -6176
    })

    assert_decimal(@binaries_scientific_tiny, %Decimal{coef: 1, exp: -6176})
    assert_decimal(@binaries_neg_tiny, %Decimal{sign: -1, coef: 1, exp: -6176})
  end

  defp assert_decimal(binaries, expected_decimal) do
    assert BSON.Decimal128.decode(binaries) == expected_decimal
  end
end
