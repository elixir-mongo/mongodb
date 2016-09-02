defmodule BatchWriteError do
  %{
    code: 0,
    errInfo: %{},
    errmsg: "",
    index: 0
  }
end
#---------------------------------------------------------------------------#
defmodule BatchWriteConcernError do
  %{
    code: 0,
    errInfo: %{},
    errmsg: ""
  }
end
#---------------------------------------------------------------------------#
defmodule BatchResult do
  %{
    ok: 1,
    n: 0,
    writeErrors: [],
    writeConcernError: nil,
    nModified: nil,
    upserted: []
  }
end
#---------------------------------------------------------------------------#
defmodule BatchFailure do
  %{
    ok: 0,
    code: 0,
    errmsg: ""
  }
end
#---------------------------------------------------------------------------#
defmodule BatchResponse do
  def defaults() do
    %{
      ok: 1,
      results: %{
        n: 0,
        writeErrors: [
        ],
        writeConcernErrors: [
        ],
        nModified: 0,
        upserted: [
        ]
      },
      error: %{
        ok: 0,
        code: 0,
        errmsg: ""
      }
    }
  end
#---------------------------------------------------------------------------#
end
