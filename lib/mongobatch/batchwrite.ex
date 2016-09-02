defmodule BatchWrite do
  def defaults() do
    %{
      collection: "",
      writes: [],
      writeConcern: %{w: 1},
      ordered: false
    }
  end
end
#---------------------------------------------------------------------------#
defmodule BatchInsertDocument do
  defstruct document: nil
end
#---------------------------------------------------------------------------#
defmodule BatchInsert do
  def defaults() do
    %{
      insert: "",
      documents:  [],
      writeConcern: %{w: 1},
      ordered: false
    }
  end
end
#---------------------------------------------------------------------------#
defmodule BatchUpdateDocument do
  defstruct q: %{}, u: %{}, multi: nil, upsert: nil
end
#---------------------------------------------------------------------------#
defmodule BatchUpdate do
  def defaults() do
    %{
      update: "",
      updates:  [],
      writeConcern: %{w: 1},
      ordered: false
    }
  end
end
#---------------------------------------------------------------------------#
defmodule BatchDeleteDocument do
  defstruct q: %{}, limit: 1
end
#---------------------------------------------------------------------------#
defmodule BatchDelete do
  def defaults() do
    %{
      delete: "",
      deletes:  [],
      writeConcern: %{w: 1},
      ordered: false
    }
  end
end
#---------------------------------------------------------------------------#
