defmodule BSONTest do
  use ExUnit.Case, async: true

  import BSON, only: [decode: 1]

  @map1 %{"hello" => "world"}
  @bin1 <<22, 0, 0, 0, 2, 104, 101, 108, 108, 111, 0, 6, 0, 0, 0, 119,
          111, 114, 108, 100, 0, 0>>

  @map2 %{"BSON" => ["awesome", 5.05, 1986]}
  @bin2 <<49, 0, 0, 0, 4, 66, 83, 79, 78, 0, 38, 0, 0, 0, 2, 48,
          0, 8, 0, 0, 0, 97, 119, 101, 115, 111, 109, 101, 0, 1, 49, 0,
          51, 51, 51, 51, 51, 51, 20, 64, 16, 50, 0, 194, 7, 0, 0, 0,
          0>>

  @map3 %{"a" => %{"b" => %{}, "c" => %{"d" => nil}}}
  @bin3 <<32, 0, 0, 0, 3, 97, 0, 24, 0, 0, 0, 3, 98, 0, 5, 0,
          0, 0, 0, 3, 99, 0, 8, 0, 0, 0, 10, 100, 0, 0, 0, 0>>

  @map4 %{"a" => [], "b" => [1, 2, 3], "c" => [1.1, "2", true]}
  @bin4 <<74, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 4, 98, 0, 26,
          0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0,
          0, 16, 50, 0, 3, 0, 0, 0, 0, 4, 99, 0, 29, 0, 0, 0,
          1, 48, 0, 154, 153, 153, 153, 153, 153, 241, 63, 2, 49, 0, 2, 0,
          0, 0, 50, 0, 8, 50, 0, 1, 0, 0>>

  @map5 %{"a" => 123.0}
  @bin5 <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 192, 94, 64, 0>>

  @map6 %{"b" => "123"}
  @bin6 <<16, 0, 0, 0, 2, 98, 0, 4, 0, 0, 0, 49, 50, 51, 0, 0>>

  @map7 %{"c" => %{}}
  @bin7 <<13, 0, 0, 0, 3, 99, 0, 5, 0, 0, 0, 0, 0>>

  @map8 %{"d" => []}
  @bin8 <<13, 0, 0, 0, 4, 100, 0, 5, 0, 0, 0, 0, 0>>

  @map9 %{"e" => %BSON.Binary{binary: <<1,2,3>>, subtype: :generic}}
  @bin9 <<16, 0, 0, 0, 5, 101, 0, 3, 0, 0, 0, 0, 1, 2, 3, 0>>

  @map10 %{"f" => %BSON.ObjectId{value: <<0,1,2,3,4,5,6,7,8,9,10,11>>}}
  @bin10 <<20, 0, 0, 0, 7, 102, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0>>

  @map11 %{"g" => true}
  @bin11 <<9, 0, 0, 0, 8, 103, 0, 1, 0>>

  @map12 %{"h" => %BSON.DateTime{utc: 12345}}
  @bin12 <<16, 0, 0, 0, 9, 104, 0, 57, 48, 0, 0, 0, 0, 0, 0, 0>>

  @map13 %{"i" => nil}
  @bin13 <<8, 0, 0, 0, 10, 105, 0, 0>>

  @map14 %{"j" => %BSON.JavaScript{code: "1 + 2"}}
  @bin14 <<18, 0, 0, 0, 13, 106, 0, 6, 0, 0, 0, 49, 32, 43, 32, 50, 0, 0>>

  @map15 %{"k" => %BSON.JavaScript{code: "a + b", scope: %{"a" => 2, "b" => 2}}}
  @bin15 <<41, 0, 0, 0, 15, 107, 0, 33, 0, 0, 0, 6, 0, 0, 0, 97, 32, 43, 32, 98, 0, 19, 0, 0, 0, 16, 97, 0, 2, 0, 0, 0, 16, 98, 0, 2, 0, 0, 0, 0, 0>>

  @map16 %{"l" => 12345}
  @bin16 <<12, 0, 0, 0, 16, 108, 0, 57, 48, 0, 0, 0>>

  @map17 %{"m" => %BSON.Timestamp{value: 12345678}}
  @bin17 <<16, 0, 0, 0, 17, 109, 0, 78, 97, 188, 0, 0, 0, 0, 0, 0>>

  @map18 %{"n" => 123456789123456}
  @bin18 <<16, 0, 0, 0, 18, 110, 0, 128, 145, 15, 134, 72, 112, 0, 0, 0>>

  @map19 %{"o" => :BSON_min}
  @bin19 <<8, 0, 0, 0, 255, 111, 0, 0>>

  @map20 %{"p" => :BSON_max}
  @bin20 <<8, 0, 0, 0, 127, 112, 0, 0>>

  test "encode" do
    assert encode(@map1)  == @bin1
    assert encode(@map2)  == @bin2
    assert encode(@map3)  == @bin3
    assert encode(@map4)  == @bin4
    assert encode(@map5)  == @bin5
    assert encode(@map6)  == @bin6
    assert encode(@map7)  == @bin7
    assert encode(@map8)  == @bin8
    assert encode(@map9)  == @bin9
    assert encode(@map10) == @bin10
    assert encode(@map11) == @bin11
    assert encode(@map12) == @bin12
    assert encode(@map13) == @bin13
    assert encode(@map14) == @bin14
    assert encode(@map15) == @bin15
    assert encode(@map16) == @bin16
    assert encode(@map17) == @bin17
    assert encode(@map18) == @bin18
    assert encode(@map19) == @bin19
    assert encode(@map20) == @bin20
  end

  test "decode" do
    assert decode(@bin1)  == @map1
    assert decode(@bin2)  == @map2
    assert decode(@bin3)  == @map3
    assert decode(@bin4)  == @map4
    assert decode(@bin5)  == @map5
    assert decode(@bin6)  == @map6
    assert decode(@bin7)  == @map7
    assert decode(@bin8)  == @map8
    assert decode(@bin9)  == @map9
    assert decode(@bin10) == @map10
    assert decode(@bin11) == @map11
    assert decode(@bin12) == @map12
    assert decode(@bin13) == @map13
    assert decode(@bin14) == @map14
    assert decode(@bin15) == @map15
    assert decode(@bin16) == @map16
    assert decode(@bin17) == @map17
    assert decode(@bin18) == @map18
    assert decode(@bin19) == @map19
    assert decode(@bin20) == @map20
  end

  test "keywords" do
    keyword = [set: [title: "x"]]
    map     = %{"set" => %{"title" => "x"}}
    encoded = <<28, 0, 0, 0, 3, 115, 101, 116, 0, 18, 0, 0, 0, 2, 116, 105, 116, 108, 101, 0, 2, 0, 0, 0, 120, 0, 0, 0>>

    assert encode(keyword) == encoded
    assert encode(map)     == encoded
    assert decode(encoded) == map
  end

  test "encode atom" do
    assert encode(%{hello: "world"}) == @bin1
  end

  test "encode atom value" do
    assert encode(%{"hello" => :world}) == @bin1
  end

  test "decode BSON symbol into string" do
    encoded = <<22, 0, 0, 0, 14, 104, 101, 108, 108, 111, 0, 6, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0>>
    assert decode(encoded) == @map1
  end

  @mapPosInf %{"a" => :inf}
  @binPosInf <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 127::little-integer-size(8), 0>>

  @mapNegInf %{"a" => :"-inf"}
  @binNegInf <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 0, 240::little-integer-size(8), 255::little-integer-size(8), 0>>

  @mapNaN %{"a" => :NaN}
  @binNaN <<16, 0, 0, 0, 1, 97, 0, 0, 0, 0, 0, 0, 0, 248::little-integer-size(8), 127::little-integer-size(8), 0>>

  test "decode float NaN" do
    assert decode(@binNaN) == @mapNaN
  end

  test "encode float NaN" do
    assert encode(@mapNaN) == @binNaN
  end

  test "decode float positive Infinity" do
    assert decode(@binPosInf) == @mapPosInf
  end

  test "encode float positive Infinity" do
    assert encode(@mapPosInf) == @binPosInf
  end

  test "decode float negative Infinity" do
    assert decode(@binNegInf) == @mapNegInf
  end

  test "encode float negative Infinity" do
    assert encode(@mapNegInf) == @binNegInf
  end

  defp encode(value) do
    value |> BSON.encode |> IO.iodata_to_binary
  end
end
