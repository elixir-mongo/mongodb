defmodule BatchWriteSorter do
  #---------------------------------------------------------------------------#
  def sortdeletes(%BatchDeleteDocument{}), do: true
  def sortdeletes(_), do: false
  #---------------------------------------------------------------------------#
  def sortupdates(%BatchUpdateDocument{}), do: true
  def sortupdates(_), do: false
  #---------------------------------------------------------------------------#
  def sortinserts(%BatchInsertDocument{}), do: true
  def sortinserts(_), do: false
  #---------------------------------------------------------------------------#
  def sortunordered(writelist) do
    # Sort write types into associated lists
    {updatestructlist, restoflist} = Enum.partition(
      writelist,
      fn(x) ->
        sortupdates(x)
      end
    )
    {deletestructlist, insertstructlist} = Enum.partition(
      restoflist,
      fn(x) ->
        sortdeletes(x)
      end
    )
    # Convert each list of structs to list of maps
    insertdoclist = Enum.map(
      insertstructlist,
      fn(x) ->
        x.document
      end
    )

    updatedoclist = Enum.map(
      updatestructlist,
      fn(x) ->
        Map.from_struct(x)
      end
    )

    deletedoclist = Enum.map(
      deletestructlist,
      fn(x) ->
        Map.from_struct(x)
      end
    )

    {insertdoclist, updatedoclist, deletedoclist}
  end
  #---------------------------------------------------------------------------#
  def sortordered(writelist) do
    # Gather into contiguous write type groups
    {lastsublistmap, contiguousdoclist} = Enum.reduce(
      writelist.writes,
      {%{writes: [], type: ""}, []},
      fn(x, {sublistmap, contiguousdoclist}) ->
        case x do
          %BatchUpdateDocument{} ->
            if ((sublistmap.type == "") || (sublistmap.type == "update")) do
              # New or same type of sublist, so...
              # Set sublist type
              sublistmap = %{sublistmap | type: "update"}
              # Add item to writes list
              {%{sublistmap | writes: Enum.concat(sublistmap.writes, [Map.from_struct(x)])}, contiguousdoclist}
            else
              # Need to change write type to update, so...
              # Move current sublistmap to orderedlist
              contiguousdoclist = Enum.concat(contiguousdoclist, [sublistmap])
              # Create new sublistmap
              {%{writes: [Map.from_struct(x)], type: "update"}, contiguousdoclist}
            end

          %BatchDeleteDocument{} ->
            if ((sublistmap.type == "") || (sublistmap.type == "delete")) do
              # New or same type of sublist, so...
              # Set sublist type
              sublistmap = %{sublistmap | type: "delete"}
              # Add item to writes list
              {%{sublistmap | writes: Enum.concat(sublistmap.writes, [Map.from_struct(x)])}, contiguousdoclist}
            else
              # Need to change write type to delete, so...
              # Move current sublistmap to orderedlist
              contiguousdoclist = Enum.concat(contiguousdoclist, [sublistmap])
              # Create new sublistmap
              {%{writes: [Map.from_struct(x)], type: "delete"}, contiguousdoclist}
            end

          %BatchInsertDocument{} ->
            if ((sublistmap.type == "") || (sublistmap.type == "insert")) do
              # New or same type of sublist, so...
              # Set sublist type
              sublistmap = %{sublistmap | type: "insert"}
              # Add item to writes list
              {%{sublistmap | writes: Enum.concat(sublistmap.writes, [x.document])}, contiguousdoclist}
            else
              # Need to change write type to insert, so...
              # Move current sublistmap to orderedlist
              contiguousdoclist = Enum.concat(contiguousdoclist, [sublistmap])
              # Create new sublistmap
              {%{writes: [x.document], type: "insert"}, contiguousdoclist}
            end

        end
      end
    )
    # Return complete ordered list of contiguous struct documents
    Enum.concat(contiguousdoclist, [lastsublistmap])
  end
  #---------------------------------------------------------------------------#
end
