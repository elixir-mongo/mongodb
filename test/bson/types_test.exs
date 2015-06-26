defmodule BSON.TypesTest do
  use ExUnit.Case, async: true

  test "inspect BSON.Binary" do
    value = %BSON.Binary{binary: <<1, 2, 3>>}
    assert inspect(value) == "#BSON.Binary<010203>"

    value = %BSON.Binary{binary: <<1, 2, 3>>, subtype: :uuid}
    assert inspect(value) == "#BSON.Binary<010203, uuid>"
  end

  test "inspect BSON.ObjectId" do
    value = %BSON.ObjectId{value: <<29, 32, 69, 244, 101, 119, 228, 28, 61, 24, 21, 215>>}
    assert inspect(value) == "#BSON.ObjectId<1d2045f46577e41c3d1815d7>"
  end

  test "inspect BSON.DateTime" do
    value = %BSON.DateTime{utc: 63602558542000}
    assert inspect(value) == "#BSON.DateTime<2015-06-26T17:22:22Z>"
  end

  test "inspect BSON.Regex" do
    value = %BSON.Regex{pattern: "abc"}
    assert inspect(value) == "#BSON.Regex<\"abc\">"

    value = %BSON.Regex{pattern: "abc", options: "i"}
    assert inspect(value) == "#BSON.Regex<\"abc\", \"i\">"
  end

  test "inspect BSON.JavaScript" do
    value = %BSON.JavaScript{code: "this === null"}
    assert inspect(value) == "#BSON.JavaScript<\"this === null\">"

    value = %BSON.JavaScript{code: "this === value", scope: %{value: nil}}
    assert inspect(value) == "#BSON.JavaScript<\"this === value\", %{value: nil}>"
  end

  test "inspect BSON.Timestamp" do
    value = %BSON.Timestamp{value: 1412180887}
    assert inspect(value) == "#BSON.Timestamp<1412180887>"
  end
end
