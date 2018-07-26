defmodule BSON.Decimal128Test do
  use ExUnit.Case, async: true
  
  @inf_binaries <<00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,120>>
  @neg_inf_binaries <<00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,248>>
  @nan_binaries <<00,00,00,00,00,00,00,00,00,00,00,00,00,00,00,124>>
  @binaries_0_001234 <<210, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 52, 48>>

  @tag :mongo_3_4
  test "BSON.Decimal128.decode/1" do
    assert BSON.Decimal128.decode(@inf_binaries) == %Decimal{coef: :inf}
    assert BSON.Decimal128.decode(@neg_inf_binaries) == %Decimal{sign: -1, coef: :inf}
    assert BSON.Decimal128.decode(@nan_binaries) == %Decimal{coef: :qNaN}
    assert BSON.Decimal128.decode(@binaries_0_001234) == %Decimal{coef: 1234, exp: -6}
  end
end