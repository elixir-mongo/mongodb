defmodule Mongo do
  @moduledoc """
  The main entry point for doing queries. All functions take a topology to
  run the query on.

  ## Generic options

  All operations take these options.

    * `:timeout` - The maximum time that the caller is allowed the to hold the
      connection’s state (ignored when using a run/transaction connection,
      default: `5_000`)
    * `:pool` - The pooling behaviour module to use, this option is required
      unless the default `DBConnection.Connection` pool is used
    * `:pool_timeout` - The maximum time to wait for a reply when making a
      synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
      connection's state (boolean, default: `true`)
    * `:log` - A function to log information about a call, either
      a 1-arity fun, `{module, function, args}` with `DBConnection.LogEntry.t`
      prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)
    * `:database` - the database to run the operation on
    * `:connect_timeout_ms` - maximum timeout for connect (default: `5_000`)

  ## Read options

  All read operations that returns a cursor take the following options
  for controlling the behaviour of the cursor.

    * `:batch_size` - Number of documents to fetch in each batch
    * `:limit` - Maximum number of documents to fetch with the cursor
    * `:read_preference` - specifies the rules for selecting a server to query

  ## Write options

  All write operations take the following options for controlling the
  write concern.

    * `:w` - The number of servers to replicate to before returning from write
      operators, a 0 value will return immediately, :majority will wait until
      the operation propagates to a majority of members in the replica set
      (Default: 1)
    * `:j` If true, the write operation will only return after it has been
      committed to journal - (Default: false)
    * `:wtimeout` - If the write concern is not satisfied in the specified
      interval, the operation returns an error
  """

  use Bitwise
  use Mongo.Messages
  alias Mongo.Query
  alias Mongo.ReadPreference
  alias Mongo.TopologyDescription
  alias Mongo.Topology
  alias Mongo.UrlParser

  @timeout 5000

  @dialyzer nowarn_function: [count_documents!: 4]

  @type conn :: DbConnection.Conn
  @type collection :: String.t()
  @opaque cursor :: Mongo.Cursor.t() | Mongo.AggregationCursor.t()
  @type result(t) :: :ok | {:ok, t} | {:error, Mongo.Error.t()}
  @type write_result(t) ::
          :ok | {:ok, t} | {:error, Mongo.Error.t()} | {:error, Mongo.WriteError.t()}
  @type result!(t) :: nil | t | no_return

  defmacrop bangify(result) do
    quote do
      case unquote(result) do
        {:ok, value} -> value
        {:error, error} -> raise error
        :ok -> nil
      end
    end
  end

  @type initial_type :: :unknown | :single | :replica_set_no_primary | :sharded

  @doc """
  Start and link to a database connection process.

  ### Options
    * `:database` - The database to use (required)
    * `:hostname` - The host to connect to (require)
    * `:port` - The port to connect to your server (default: 27017)
    * `:url` - A mongo connection url. Can be used in place of `:hostname` and
    * `:socket_dir` - Connect to MongoDB via UNIX sockets in the given directory.
      The socket name is derived based on the port. This is the preferred method
      for configuring sockets and it takes precedence over the hostname. If you
      are connecting to a socket outside of the MongoDB convection, use
      `:socket` instead.
    * `:socket` - Connect to MongoDB via UNIX sockets in the given path.
      This option takes precedence over `:hostname` and `:socket_dir`.
    * `:database` (optional)
    * `:seeds` - A list of host names in the cluster. Can be used in place of
      `:hostname` (optional)
    * `:username` - The User to connect with (optional)
    * `:password` - The password to connect with (optional)
    * `:auth` - List of additional users to authenticate as a keyword list with
      `:username` and `:password` keys (optional)
    * `:auth_source` - The database to authenticate against
    * `:set_name` - The name of the replica set to connect to (required if
    connecting to a replica set)
    * `:type` - a hint of the topology type. See `t:initial_type/0` for
      valid values (default: `:unknown`)
    * `:pool` - The pool module to use, see `DBConnection` for pool dependent
      options, this option must be included with all requests contacting the
      pool if not `DBConnection.Connection` (default: `DBConnection.Connection`)
    * `:idle` - The idle strategy, `:passive` to avoid checkin when idle and
    * `:active` to checking when idle (default: `:passive`)
    * `:idle_timeout` - The idle timeout to ping the database (default: `1_000`)
    * `:connect_timeout_ms` - The maximum timeout for the initial connection
      (default: `5_000`)
    * `:backoff_min` - The minimum backoff interval (default: `1_000`)
    * `:backoff_max` - The maximum backoff interval (default: `30_000`)
    * `:backoff_type` - The backoff strategy, `:stop` for no backoff and to
      stop, `:exp` of exponential, `:rand` for random and `:ran_exp` for random
      exponential (default: `:rand_exp`)
    * `:after_connect` - A function to run on connect use `run/3`. Either a
      1-arity fun, `{module, function, args}` with `DBConnection.t`, prepended
      to `args` or `nil` (default: `nil`)
    * `:auth_mechanism` - options for the mongo authentication mechanism,
      currently only supports `:x509` atom as a value
    * `:ssl` - Set to `true` if ssl should be used (default: `false`)
    * `:ssl_opts` - A list of ssl options, see the ssl docs

  ### Error Reasons
    * `:single_topology_multiple_hosts` - A topology of `:single` was set
      but multiple hosts were given
    * `:set_name_bad_topology` - A `:set_name` was given but the topology was
      set to something other than `:replica_set_no_primary` or `:single`
  """
  @spec start_link(Keyword.t()) :: {:ok, pid} | {:error, Mongo.Error.t() | atom}
  def start_link(opts) do
    opts
    |> UrlParser.parse_url()
    |> Mongo.ConfigHide.mask_password()
    |> Topology.start_link()
  end

  def child_spec(opts, child_opts \\ []) do
    child_opts
    |> Map.new()
    |> Map.merge(%{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    })
  end

  @doc """
  Generates a new `BSON.ObjectId`.
  """
  @spec object_id :: BSON.ObjectId.t()
  def object_id do
    Mongo.IdServer.new()
  end

  @doc """
  Performs aggregation operation using the aggregation pipeline.

  ## Options

    * `:allow_disk_use` - Enables writing to temporary files (Default: false)
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and
    * `:max_time` - Specifies a time limit in milliseconds
    * `:use_cursor` - Use a cursor for a batched response (Default: true)
  """
  @spec aggregate(GenServer.server(), collection, [BSON.document()], Keyword.t()) :: cursor
  def aggregate(topology_pid, coll, pipeline, opts \\ []) do
    query =
      [
        aggregate: coll,
        pipeline: pipeline,
        allowDiskUse: opts[:allow_disk_use],
        collation: opts[:collation],
        maxTimeMS: opts[:max_time],
        hint: opts[:hint]
      ]
      |> filter_nils()

    wv_query = %Query{action: :wire_version}

    with {:ok, query} <- Mongo.Session.add_session(query, opts[:session]),
         {:ok, conn, _, _} <- select_server(topology_pid, :read, opts),
         {:ok, _query, version} <- DBConnection.execute(conn, wv_query, [], defaults(opts)) do
      cursor? = version >= 1 and Keyword.get(opts, :use_cursor, true)
      opts = Keyword.drop(opts, ~w(allow_disk_use max_time use_cursor)a)

      if cursor? do
        query = query ++ [cursor: filter_nils(%{batchSize: opts[:batch_size]})]
        aggregation_cursor(conn, "$cmd", query, nil, opts)
      else
        query = query ++ [cursor: %{}]
        aggregation_cursor(conn, "$cmd", query, nil, opts)
      end
    end
  end

  @doc """
  Finds a document and updates it (using atomic modifiers).

  ## Options

    * `:bypass_document_validation` -  Allows the write to opt-out of document
      level validation
    * `:max_time` -  The maximum amount of time to allow the query to run (in MS)
    * `:projection` -  Limits the fields to return for all matching documents.
    * `:return_document` - Returns the replaced or inserted document rather than
       the original. Values are :before or :after. (default is :before)
    * `:sort` - Determines which document the operation modifies if the query
      selects multiple documents.
    * `:upsert` -  Create a document if no document matches the query or updates
      the document.
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and
  """
  @spec find_one_and_update(
          GenServer.server(),
          collection,
          BSON.document(),
          BSON.document(),
          Keyword.t()
        ) :: result(BSON.document()) | {:ok, nil}
  def find_one_and_update(topology_pid, coll, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)

    query =
      [
        findAndModify: coll,
        query: filter,
        update: update,
        bypassDocumentValidation: opts[:bypass_document_validation],
        maxTimeMS: opts[:max_time],
        fields: opts[:projection],
        new: should_return_new(opts[:return_document]),
        sort: opts[:sort],
        upsert: opts[:upsert],
        collation: opts[:collation]
      ]
      |> filter_nils()

    opts =
      Keyword.drop(
        opts,
        ~w(bypass_document_validation max_time projection return_document sort upsert collation)a
      )

    with {:ok, query} <- Mongo.Session.add_session(query, opts[:session]),
         {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- direct_command(conn, query, opts) do
      {:ok,
       %Mongo.FindAndModifyResult{
         value: doc["value"],
         matched_count: doc["lastErrorObject"]["n"],
         updated_existing: doc["lastErrorObject"]["updatedExisting"],
         upserted_id: doc["lastErrorObject"]["upserted"]
       }}
    end
  end

  @doc """
  Finds a document and replaces it.

  ## Options

    * `:bypass_document_validation` -  Allows the write to opt-out of document
      level validation
    * `:max_time` -  The maximum amount of time to allow the query to run (in MS)
    * `:projection` -  Limits the fields to return for all matching documents.
    * `:return_document` - Returns the replaced or inserted document rather than
      the original. Values are :before or :after. (default is :before)
    * `:sort` - Determines which document the operation modifies if the query
      selects multiple documents.
    * `:upsert` -  Create a document if no document matches the query or updates
      the document.
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and
      higher.
  """
  @spec find_one_and_replace(
          GenServer.server(),
          collection,
          BSON.document(),
          BSON.document(),
          Keyword.t()
        ) :: result(BSON.document())
  def find_one_and_replace(topology_pid, coll, filter, replacement, opts \\ []) do
    _ = modifier_docs(replacement, :replace)

    query =
      filter_nils(
        findAndModify: coll,
        query: filter,
        update: replacement,
        bypassDocumentValidation: opts[:bypass_document_validation],
        maxTimeMS: opts[:max_time],
        fields: opts[:projection],
        new: should_return_new(opts[:return_document]),
        sort: opts[:sort],
        upsert: opts[:upsert],
        collation: opts[:collation]
      )

    opts =
      Keyword.drop(
        opts,
        ~w(bypass_document_validation max_time projection return_document sort upsert collation)a
      )

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- direct_command(conn, query, opts) do
      {:ok,
       %Mongo.FindAndModifyResult{
         value: doc["value"],
         matched_count: doc["lastErrorObject"]["n"],
         updated_existing: doc["lastErrorObject"]["updatedExisting"],
         upserted_id: doc["lastErrorObject"]["upserted"]
       }}
    end
  end

  defp should_return_new(:after), do: true
  defp should_return_new(:before), do: false
  defp should_return_new(_), do: false

  @doc """
  Finds a document and deletes it.

  ## Options

    * `:max_time` -  The maximum amount of time to allow the query to run (in MS)
    * `:projection` -  Limits the fields to return for all matching documents.
    * `:sort` - Determines which document the operation modifies if the query selects multiple documents.
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and higher.
  """
  @spec find_one_and_delete(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result(BSON.document())
  def find_one_and_delete(topology_pid, coll, filter, opts \\ []) do
    query =
      filter_nils(
        findAndModify: coll,
        query: filter,
        remove: true,
        maxTimeMS: opts[:max_time],
        fields: opts[:projection],
        sort: opts[:sort],
        collation: opts[:collation]
      )

    opts = Keyword.drop(opts, ~w(max_time projection sort collation)a)

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- direct_command(conn, query, opts),
         do: {:ok, doc["value"]}
  end

  @doc false
  @spec count(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result(non_neg_integer)
  def count(topology_pid, coll, filter, opts \\ []) do
    query =
      filter_nils(
        count: coll,
        query: filter |> normalize_filter(),
        limit: opts[:limit],
        skip: opts[:skip],
        hint: opts[:hint],
        collation: opts[:collation]
      )

    opts = Keyword.drop(opts, ~w(limit skip hint collation)a)

    # Mongo 2.4 and 2.6 returns a float
    with {:ok, doc} <- command(topology_pid, query, opts),
         do: {:ok, trunc(doc["n"])}
  end

  @doc false
  @spec count!(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result!(non_neg_integer)
  def count!(topology_pid, coll, filter, opts \\ []) do
    bangify(count(topology_pid, coll, filter, opts))
  end

  @doc """
  Returns the count of documents that would match a find/4 query.

  ## Options
    * `:limit` - Maximum number of documents to fetch with the cursor
    * `:skip` - Number of documents to skip before returning the first
  """
  @spec count_documents(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result(non_neg_integer)
  def count_documents(topology_pid, coll, filter, opts \\ []) do
    pipeline =
      [
        {"$match", filter |> normalize_filter()},
        {"$skip", opts[:skip]},
        {"$limit", opts[:limit]},
        {"$group", %{"_id" => nil, "n" => %{"$sum" => 1}}}
      ]
      |> filter_nils
      |> Enum.map(&List.wrap/1)

    documents =
      topology_pid
      |> Mongo.aggregate(coll, pipeline, opts)
      |> Enum.to_list()

    case documents do
      [%{"n" => count}] -> {:ok, count}
      [] -> {:ok, 0}
    end
  end

  # As of at least MongoDB v4.4.3 filters are expected as a map but it seems like in the past a
  # keyword list was accepted.  `normalize_filter` is provided to iron out the differences and
  # provide backwards compatibilitys
  defp normalize_filter(filter) when is_map(filter), do: filter
  defp normalize_filter(filter), do: filter |> Enum.into(%{})

  @doc """
  Similar to `count_documents/4` but unwraps the result and raises on error.
  """
  @spec count_documents!(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result!(non_neg_integer)
  def count_documents!(topology_pid, coll, filter, opts \\ []) do
    bangify(count_documents(topology_pid, coll, filter, opts))
  end

  @doc """
  Estimate the number of documents in a collection using collection metadata.
  """
  @spec estimated_document_count(GenServer.server(), collection, Keyword.t()) ::
          result(non_neg_integer)
  def estimated_document_count(topology_pid, coll, opts) do
    opts = Keyword.drop(opts, [:skip, :limit, :hint, :collation])
    count(topology_pid, coll, %{}, opts)
  end

  @doc """
  Similar to `estimated_document_count/3` but unwraps the result and raises on
  error.
  """
  @spec estimated_document_count!(GenServer.server(), collection, Keyword.t()) ::
          result!(non_neg_integer)
  def estimated_document_count!(topology_pid, coll, opts) do
    bangify(estimated_document_count(topology_pid, coll, opts))
  end

  @doc """
  Finds the distinct values for a specified field across a collection.

  ## Options

    * `:max_time` - Specifies a time limit in milliseconds
    * `:collation` - Optionally specifies a collation to use in MongoDB 3.4 and
  """
  @spec distinct(GenServer.server(), collection, String.t() | atom, BSON.document(), Keyword.t()) ::
          result([BSON.t()])
  def distinct(topology_pid, coll, field, filter, opts \\ []) do
    query =
      filter_nils(
        distinct: coll,
        key: field,
        query: filter,
        collation: opts[:collation],
        maxTimeMS: opts[:max_time]
      )

    opts = Keyword.drop(opts, ~w(max_time)a)

    with {:ok, conn, _, _} <- select_server(topology_pid, :read, opts),
         {:ok, doc} <- direct_command(conn, query, opts),
         do: {:ok, doc["values"]}
  end

  @doc """
  Similar to `distinct/5` but unwraps the result and raises on error.
  """
  @spec distinct!(GenServer.server(), collection, String.t() | atom, BSON.document(), Keyword.t()) ::
          result!([BSON.t()])
  def distinct!(topology_pid, coll, field, filter, opts \\ []) do
    bangify(distinct(topology_pid, coll, field, filter, opts))
  end

  @doc """
  Selects documents in a collection and returns a cursor for the selected
  documents.

  ## Options

    * `:comment` - Associates a comment to a query
    * `:cursor_type` - Set to :tailable or :tailable_await to return a tailable
      cursor
    * `:max_time` - Specifies a time limit in milliseconds
    * `:modifiers` - Meta-operators modifying the output or behavior of a query,
      see http://docs.mongodb.org/manual/reference/operator/query-modifier/
    * `:cursor_timeout` - Set to false if cursor should not close after 10
      minutes (Default: true)
    * `:sort` - Sorts the results of a query in ascending or descending order
    * `:projection` - Limits the fields to return for all matching document
    * `:skip` - The number of documents to skip before returning (Default: 0)
  """
  @spec find(GenServer.server(), collection, BSON.document(), Keyword.t()) :: cursor
  def find(topology_pid, coll, filter, opts \\ []) do
    query =
      [
        {"$comment", opts[:comment]},
        {"$maxTimeMS", opts[:max_time]},
        {"$orderby", opts[:sort]}
      ] ++ Enum.into(opts[:modifiers] || [], [])

    query = filter_nils(query)

    query =
      if query == [] do
        filter
      else
        filter = normalize_doc(filter)
        filter = if List.keymember?(filter, "$query", 0), do: filter, else: [{"$query", filter}]
        filter ++ query
      end

    select = opts[:projection]

    opts =
      if Keyword.get(opts, :cursor_timeout, true),
        do: opts,
        else: [{:no_cursor_timeout, true} | opts]

    drop = ~w(comment max_time modifiers sort cursor_type projection cursor_timeout)a
    opts = cursor_type(opts[:cursor_type]) ++ Keyword.drop(opts, drop)

    with {:ok, conn, slave_ok, _} <- select_server(topology_pid, :read, opts),
         opts = Keyword.put(opts, :slave_ok, slave_ok),
         do: cursor(conn, coll, query, select, opts)
  end

  @doc """
  Selects a single document in a collection and returns either a document
  or nil.

  If multiple documents satisfy the query, this method returns the first document
  according to the natural order which reflects the order of documents on the disk.

  ## Options

    * `:comment` - Associates a comment to a query
    * `:cursor_type` - Set to :tailable or :tailable_await to return a tailable
      cursor
    * `:max_time` - Specifies a time limit in milliseconds
    * `:modifiers` - Meta-operators modifying the output or behavior of a query,
      see http://docs.mongodb.org/manual/reference/operator/query-modifier/
    * `:cursor_timeout` - Set to false if cursor should not close after 10
      minutes (Default: true)
    * `:projection` - Limits the fields to return for all matching document
    * `:skip` - The number of documents to skip before returning (Default: 0)
  """
  @spec find_one(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          BSON.document() | nil
  def find_one(conn, coll, filter, opts \\ []) do
    opts =
      opts
      |> Keyword.delete(:order_by)
      |> Keyword.delete(:sort)
      |> Keyword.put(:limit, 1)
      |> Keyword.put(:batch_size, 1)

    with [elem] <- Enum.to_list(find(conn, coll, filter, opts)) do
      elem
    else
      [] -> nil
      error -> error
    end
  end

  @doc false
  def raw_find(conn, coll, query, select, opts) do
    query = filter_nils([
      find: coll,
      filter: query,
      projection: select,
      batchSize: opts[:batch_size],
      skip: opts[:skip],
    ])

    opts = Keyword.drop(opts, [:skip, :batch_size])

    with {:ok, %{"cursor" => %{"id" => id, "firstBatch" => docs}}} <-
           direct_command(conn, query, opts) do
      {:ok, %{from: 0, num: Enum.count(docs), cursor_id: id, docs: docs}}
    end
  end

  @doc false
  def get_more(conn, coll, cursor, opts) do
    query = filter_nils([
      getMore: cursor,
      collection: coll,
      batchSize: opts[:batch_size],
    ])

    opts = Keyword.drop(opts, [:batch_size])

    with {:ok, %{"cursor" => %{"id" => id, "nextBatch" => docs}}} <-
           direct_command(conn, query, opts) do
      {:ok, %{from: 0, num: Enum.count(docs), cursor_id: id, docs: docs}}
    end
  end

  @doc false
  def kill_cursors(conn, cursor_ids, opts) do
    query = %Query{action: :kill_cursors, extra: cursor_ids}

    with {:ok, _query, :ok} <- DBConnection.execute(conn, query, [], defaults(opts)),
         do: :ok
  end

  @doc """
  Issue a database command. If the command has parameters use a keyword
  list for the document because the "command key" has to be the first
  in the document.
  """
  @spec command(GenServer.server(), BSON.document(), Keyword.t()) :: result(BSON.document())
  def command(topology_pid, query, opts \\ []) do
    rp = ReadPreference.defaults(%{mode: :primary})
    rp_opts = [read_preference: Keyword.get(opts, :read_preference, rp)]

    with {:ok, conn, slave_ok, _} <- select_server(topology_pid, :read, rp_opts) do
      opts = Keyword.put(opts, :slave_ok, slave_ok)
      direct_command(conn, query, opts)
    end
  end

  @doc false
  @spec direct_command(pid, BSON.document(), Keyword.t()) ::
          {:ok, BSON.document() | nil} | {:error, Mongo.Error.t()}
  def direct_command(conn, query, opts \\ []) do
    params = [query]
    query = %Query{action: :command}

    with {:ok, _query, reply} <- DBConnection.execute(conn, query, params, defaults(opts)) do
      case reply do
        op_reply(flags: flags, docs: [%{"$err" => reason, "code" => code}])
        when (@reply_query_failure &&& flags) != 0 ->
          {:error, Mongo.Error.exception(message: reason, code: code)}

        op_reply(flags: flags) when (@reply_cursor_not_found &&& flags) != 0 ->
          {:error, Mongo.Error.exception(message: "cursor not found")}

        op_reply(docs: [%{"ok" => 0.0, "errmsg" => reason} = error]) ->
          {:error, %Mongo.Error{message: "command failed: #{reason}", code: error["code"]}}

        op_reply(docs: [%{"ok" => ok} = doc]) when ok == 1 ->
          Mongo.Session.update_session(doc, opts[:session])

          {:ok, doc}

        # TODO: Check if needed
        op_reply(docs: []) ->
          {:ok, nil}
      end
    end
  end

  @doc """
  Similar to `command/3` but unwraps the result and raises on error.
  """
  @spec command!(GenServer.server(), BSON.document(), Keyword.t()) :: result!(BSON.document())
  def command!(topology_pid, query, opts \\ []) do
    bangify(command(topology_pid, query, opts))
  end

  @doc """
  Insert a single document into the collection.

  If the document is missing the `_id` field or it is `nil`, an ObjectId
  will be generated, inserted into the document, and returned in the result struct.

  ## Examples

      Mongo.insert_one(pid, "users", %{first_name: "John", last_name: "Smith"})
  """
  @spec insert_one(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          write_result(Mongo.InsertOneResult.t())
  def insert_one(topology_pid, coll, doc, opts \\ []) do
    assert_single_doc!(doc)
    {[id], [doc]} = assign_ids([doc])

    write_concern =
      filter_nils(%{
        w: Keyword.get(opts, :w),
        j: Keyword.get(opts, :j),
        wtimeout: Keyword.get(opts, :wtimeout)
      })

    query =
      [
        insert: coll,
        documents: [doc],
        ordered: Keyword.get(opts, :ordered),
        writeConcern: write_concern,
        bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
      ]
      |> filter_nils()

    with {:ok, query} <- Mongo.Session.add_session(query, opts[:session]),
         {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- direct_command(conn, query, opts) do
      case doc do
        %{"writeErrors" => _} ->
          {:error,
           %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}

        %{"writeConcernError" => writeConcernError} ->
          {:error,
           %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: [writeConcernError]}}

        _ ->
          case Map.get(write_concern, :w) do
            0 ->
              {:ok, %Mongo.InsertOneResult{acknowledged: false}}

            _ ->
              {:ok, %Mongo.InsertOneResult{inserted_id: id}}
          end
      end
    end
  end

  @doc """
  Similar to `insert_one/4` but unwraps the result and raises on error.
  """
  @spec insert_one!(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result!(Mongo.InsertOneResult.t())
  def insert_one!(topology_pid, coll, doc, opts \\ []) do
    bangify(insert_one(topology_pid, coll, doc, opts))
  end

  @doc """
  Insert multiple documents into the collection.

  If any of the documents is missing the `_id` field or it is `nil`, an ObjectId
  will be generated, and insertd into the document.
  Ids of all documents will be returned in the result struct.

  ## Options

    * `:continue_on_error` - even if insert fails for one of the documents
      continue inserting the remaining ones (default: `false`)
    * `:ordered` - A boolean specifying whether the mongod instance should
      perform an ordered or unordered insert. (default: `true`)

  ## Examples

      Mongo.insert_many(pid, "users", [%{first_name: "John", last_name: "Smith"}, %{first_name: "Jane", last_name: "Doe"}])
  """
  @spec insert_many(GenServer.server(), collection, [BSON.document()], Keyword.t()) ::
          write_result(Mongo.InsertManyResult.t())
  def insert_many(topology_pid, coll, docs, opts \\ []) do
    assert_many_docs!(docs)
    {ids, docs} = assign_ids(docs)

    write_concern =
      filter_nils(%{
        w: Keyword.get(opts, :w),
        j: Keyword.get(opts, :j),
        wtimeout: Keyword.get(opts, :wtimeout)
      })

    query =
      filter_nils(
        insert: coll,
        documents: docs,
        ordered: Keyword.get(opts, :ordered),
        writeConcern: write_concern,
        bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
      )

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- direct_command(conn, query, opts) do
      case doc do
        %{"writeErrors" => _} ->
          {:error,
           %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: doc["writeErrors"]}}

        _ ->
          case Map.get(write_concern, :w) do
            0 ->
              {:ok, %Mongo.InsertManyResult{acknowledged: false}}

            _ ->
              {:ok, %Mongo.InsertManyResult{inserted_ids: ids}}
          end
      end
    end
  end

  @doc """
  Similar to `insert_many/4` but unwraps the result and raises on error.
  """
  @spec insert_many!(GenServer.server(), collection, [BSON.document()], Keyword.t()) ::
          result!(Mongo.InsertManyResult.t())
  def insert_many!(topology_pid, coll, docs, opts \\ []) do
    bangify(insert_many(topology_pid, coll, docs, opts))
  end

  @doc """
  Remove a document matching the filter from the collection.
  """
  @spec delete_one(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          write_result(Mongo.DeleteResult.t())
  def delete_one(topology_pid, coll, filter, opts \\ []) do
    do_delete(topology_pid, coll, filter, 1, opts)
  end

  @doc """
  Similar to `delete_one/4` but unwraps the result and raises on error.
  """
  @spec delete_one!(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result!(Mongo.DeleteResult.t())
  def delete_one!(topology_pid, coll, filter, opts \\ []) do
    bangify(delete_one(topology_pid, coll, filter, opts))
  end

  @doc """
  Remove all documents matching the filter from the collection.
  """
  @spec delete_many(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          write_result(Mongo.DeleteResult.t())
  def delete_many(topology_pid, coll, filter, opts \\ []) do
    do_delete(topology_pid, coll, filter, 0, opts)
  end

  @doc """
  Similar to `delete_many/4` but unwraps the result and raises on error.
  """
  @spec delete_many!(GenServer.server(), collection, BSON.document(), Keyword.t()) ::
          result!(Mongo.DeleteResult.t())
  def delete_many!(topology_pid, coll, filter, opts \\ []) do
    bangify(delete_many(topology_pid, coll, filter, opts))
  end

  def delete(topology_pid, coll, deletes, opts) do
    write_concern =
      filter_nils(%{
        w: Keyword.get(opts, :w),
        j: Keyword.get(opts, :j),
        wtimeout: Keyword.get(opts, :wtimeout)
      })

    normalised_deletes = deletes |> normalise_deletes()

    query =
      filter_nils(
        delete: coll,
        deletes: normalised_deletes,
        ordered: Keyword.get(opts, :ordered),
        writeConcern: write_concern
      )

    with {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- direct_command(conn, query, opts) do
      case doc do
        %{"writeErrors" => write_errors} ->
          {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: write_errors}}

        %{"n" => n} ->
          {:ok, %Mongo.DeleteResult{deleted_count: n}}

        %{"ok" => ok} when ok == 1 ->
          {:ok, %Mongo.DeleteResult{acknowledged: false}}
      end
    end
  end

  defp normalise_deletes([[{_, _} | _] | _] = deletes) do
    deletes
    |> Enum.map(&normalise_delete/1)
  end

  defp normalise_deletes(deletes), do: normalise_deletes([deletes])

  defp normalise_delete(delete) do
    delete
    |> Enum.map(fn
      {:query, query} -> {:q, query}
      other -> other
    end)
    |> filter_nils()
  end

  defp do_delete(topology_pid, coll, filter, limit, opts) do
    delete = [
      query: filter,
      limit: limit,
      collation: Keyword.get(opts, :collation)
    ]

    delete(topology_pid, coll, delete, opts)
  end

  @doc """
  Replace a single document matching the filter with the new document.

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec replace_one(GenServer.server(), collection, BSON.document(), BSON.document(), Keyword.t()) ::
          write_result(Mongo.UpdateResult.t())
  def replace_one(topology_pid, coll, filter, replacement, opts \\ []) do
    _ = modifier_docs(replacement, :replace)

    do_update(topology_pid, coll, filter, replacement, false, opts)
  end

  @doc """
  Similar to `replace_one/5` but unwraps the result and raises on error.
  """
  @spec replace_one!(
          GenServer.server(),
          collection,
          BSON.document(),
          BSON.document(),
          Keyword.t()
        ) :: result!(Mongo.UpdateResult.t())
  def replace_one!(topology_pid, coll, filter, replacement, opts \\ []) do
    bangify(replace_one(topology_pid, coll, filter, replacement, opts))
  end

  @doc """
  Update a single document matching the filter.

  Uses MongoDB update operators to specify the updates. For more information
  please refer to the
  [MongoDB documentation](http://docs.mongodb.org/manual/reference/operator/update/)

  Example:

      Mongo.update_one(MongoPool,
        "my_test_collection",
        %{"filter_field": "filter_value"},
        %{"$set": %{"modified_field": "new_value"}})

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec update_one(GenServer.server(), collection, BSON.document(), BSON.document(), Keyword.t()) ::
          write_result(Mongo.UpdateResult.t())
  def update_one(topology_pid, coll, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)

    do_update(topology_pid, coll, filter, update, false, opts)
  end

  @doc """
  Similar to `update_one/5` but unwraps the result and raises on error.
  """
  @spec update_one!(GenServer.server(), collection, BSON.document(), BSON.document(), Keyword.t()) ::
          result!(Mongo.UpdateResult.t())
  def update_one!(topology_pid, coll, filter, update, opts \\ []) do
    bangify(update_one(topology_pid, coll, filter, update, opts))
  end

  @doc """
  Update all documents matching the filter.

  Uses MongoDB update operators to specify the updates. For more information
  please refer to the
  [MongoDB documentation](http://docs.mongodb.org/manual/reference/operator/update/)

  ## Options

    * `:upsert` - if set to `true` creates a new document when no document
      matches the filter (default: `false`)
  """
  @spec update_many(GenServer.server(), collection, BSON.document(), BSON.document(), Keyword.t()) ::
          write_result(Mongo.UpdateResult.t())
  def update_many(topology_pid, coll, filter, update, opts \\ []) do
    _ = modifier_docs(update, :update)

    do_update(topology_pid, coll, filter, update, true, opts)
  end

  @doc """
  Similar to `update_many/5` but unwraps the result and raises on error.
  """
  @spec update_many!(
          GenServer.server(),
          collection,
          BSON.document(),
          BSON.document(),
          Keyword.t()
        ) :: result!(Mongo.UpdateResult.t())
  def update_many!(topology_pid, coll, filter, update, opts \\ []) do
    bangify(update_many(topology_pid, coll, filter, update, opts))
  end

  @doc """
  Performs one or more update operations.

  This function is especially useful for more complex update operations (e.g.
  upserting multiple documents). For more straightforward use cases you may
  prefer to use these higher level APIs:

  * `update_one/5`
  * `update_one!/5`
  * `update_many/5`
  * `update_many!5`

  Each update in `updates` may be specified using either the short-hand
  Mongo-style syntax (in reference to their docs) or using a long-hand, Elixir
  friendly syntax.

  See
  https://docs.mongodb.com/manual/reference/command/update/#update-statements

  e.g. long-hand `query` becomes short-hand `q`, snake case `array_filters`
  becomes `arrayFilters`
  """
  def update(topology_pid, coll, updates, opts) do
    write_concern =
      filter_nils(%{
        w: Keyword.get(opts, :w),
        j: Keyword.get(opts, :j),
        wtimeout: Keyword.get(opts, :wtimeout)
      })

    normalised_updates = updates |> normalise_updates()

    query =
      [
        update: coll,
        updates: normalised_updates,
        ordered: Keyword.get(opts, :ordered),
        writeConcern: write_concern,
        bypassDocumentValidation: Keyword.get(opts, :bypass_document_validation)
      ]
      |> filter_nils()

    with {:ok, query} <- Mongo.Session.add_session(query, opts[:session]),
         {:ok, conn, _, _} <- select_server(topology_pid, :write, opts),
         {:ok, doc} <- direct_command(conn, query, opts) do
      case doc do
        %{"writeErrors" => write_errors} ->
          {:error, %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: write_errors}}

        %{"n" => n, "nModified" => n_modified} ->
          {:ok,
           %Mongo.UpdateResult{
             matched_count: n,
             modified_count: n_modified,
             upserted_ids: upserted_ids(doc["upserted"])
           }}

        %{"ok" => ok} when ok == 1 ->
          {:ok, %Mongo.UpdateResult{acknowledged: false}}
      end
    end
  end

  defp normalise_updates([[{_, _} | _] | _] = updates) do
    updates
    |> Enum.map(&normalise_update/1)
  end

  defp normalise_updates(updates), do: normalise_updates([updates])

  defp normalise_update(update) do
    update
    |> Enum.map(fn
      {:query, query} -> {:q, query}
      {:update, update} -> {:u, update}
      {:updates, update} -> {:u, update}
      {:array_filters, array_filters} -> {:arrayFilters, array_filters}
      other -> other
    end)
    |> filter_nils()
  end

  defp mongo_update(filter, update, opts) do
    [
      q: filter,
      u: update,
      upsert: opts |> Keyword.get(:upsert),
      multi: opts |> Keyword.get(:multi),
      collation: opts |> Keyword.get(:collation),
      arrayFilters: opts |> Keyword.get(:array_filters),
      hint: opts |> Keyword.get(:hint)
    ]
    |> filter_nils()
  end

  # do_update/6 was in existence before `update/5` and now just serves to
  # translate between calling functions and `update/5`.  It could eventually be
  # factored out.  2020-08-26 JP.
  defp do_update(topology_pid, coll, filter, update, multi, opts) do
    update = mongo_update(filter, update, opts |> Keyword.put(:multi, multi))

    update(topology_pid, coll, update, opts)
  end

  defp upserted_ids(nil), do: nil
  defp upserted_ids(docs), do: Enum.map(docs, fn d -> d["_id"] end)

  @doc """
  Creates one or more `indexes` for the specified collection `coll`.

  See
  https://docs.mongodb.com/manual/reference/method/db.collection.createIndexes/#mongodb-method-db.collection.createIndexes
  for the syntax of `indexes`.
  """
  @spec create_indexes(GenServer.server(), String.t(), [Keyword.t()], Keyword.t()) ::
          result(Mongo.CreateIndexesResult.t())
  def create_indexes(topology_pid, coll, indexes, opts \\ []) do
    with {:ok, result} <-
           Mongo.command(topology_pid, [createIndexes: coll, indexes: indexes], opts) do
      {:ok,
       %Mongo.CreateIndexesResult{
         commit_quorum: result["commitQuorum"],
         created_collection_automatically: !!result["createdCollectionAutomatically"],
         num_indexes_after: result["numIndexesAfter"],
         num_indexes_before: result["numIndexesBefore"]
       }}
    end
  end

  @doc """
  Returns a cursor to enumerate all indexes.
  """
  @spec list_indexes(GenServer.server(), String.t(), Keyword.t()) :: cursor
  def list_indexes(topology_pid, coll, opts \\ []) do
    with {:ok, conn, _, _} <- select_server(topology_pid, :read, opts) do
      aggregation_cursor(conn, "$cmd", [listIndexes: coll], nil, opts)
    end
  end

  @doc """
  Convenience function returning a cursor with the names of the indexes.
  """
  @spec list_index_names(GenServer.server(), String.t(), Keyword.t()) :: %Stream{}
  def list_index_names(topology_pid, coll, opts \\ []) do
    list_indexes(topology_pid, coll, opts)
    |> Stream.map(fn %{"name" => name} -> name end)
  end

  @doc """
  Drops the specified `index` name in the collection `coll`.

  To drop multiple indexes at once pass a list of indexes to `index`.  To drop all indexes except
  that of `_id` pass "*" to `index`.

  See https://docs.mongodb.com/manual/reference/command/dropIndexes/#dropindexes
  """
  @spec drop_index(GenServer.server(), String.t(), String.t() | [String.t()], Keyword.t()) ::
          result(Mongo.DropIndexResult.t())
  def drop_index(topology_pid, coll, index, opts \\ []) do
    with {:ok, result} <- Mongo.command(topology_pid, [dropIndexes: coll, index: index], opts) do
      {:ok, %Mongo.DropIndexResult{num_indexes_was: result["nIndexesWas"]}}
    end
  end

  @doc """
  Lists collection names
  """
  @spec show_collections(GenServer.server(), Keyword.t()) :: cursor
  def show_collections(topology_pid, opts \\ []) do
    ##
    # from the specs
    # https://github.com/mongodb/specifications/blob/f4bb783627e7ed5c4095c5554d35287956ef8970/source/enumerate-collections.rst#post-mongodb-280-rc3-versions
    #
    # In versions 2.8.0-rc3 and later, the listCollections command returns a cursor!
    #
    with {:ok, conn, _, _} <- select_server(topology_pid, :read, opts) do
      aggregation_cursor(conn, "$cmd", [listCollections: 1], nil, opts)
      |> Stream.filter(fn coll -> coll["type"] == "collection" end)
      |> Stream.map(fn coll -> coll["name"] end)
    end
  end

  @doc """
  Start new session for given `topology_pid`.

  ## Options

  - `:causal_consistency` - whether the causal consistency should be persisted within
    session. Default to `true`.
  - `:read_concern` - what should be the level for read consistency in session. Should
    be map with value `:level` that is one of the described in [*Read Concern*][rc]
    documentation. Applied only when `:casual_consistency` is set to `true`. Bu default
    uses cluster configuration.
  - `:retry_writes` - whether retryable faliures should be retried. Defaults to `true`.

  `:causal_consistency` can be set only during the session creation, but the `:read_concern`
  can be set for each transaction independently.

  [rc]: https://docs.mongodb.com/manual/reference/read-concern/index.html
  """
  @spec start_session(GenServer.server(), keyword()) ::
          {:ok, Mongo.Session.session()} | {:error, term()}
  def start_session(topology_pid, opts \\ []) do
    Mongo.SessionPool.checkout(topology_pid, opts)
  end

  @spec with_session(GenServer.server(), (Mongo.Session.session() -> return)) :: return
        when return: term()
  @spec with_session(GenServer.server(), keyword(), (Mongo.Session.session() -> return)) :: return
        when return: term()
  def with_session(topology_pid, opts \\ [], func) do
    with {:ok, pid} <- start_session(topology_pid, opts) do
      try do
        func.(pid)
      after
        Mongo.Session.end_session(pid)
      end
    end
  end

  def select_server(topology_pid, type, opts \\ []) do
    with {:ok, servers, slave_ok, mongos?} <-
           select_servers(topology_pid, type, opts) do
      if Enum.empty?(servers) do
        {:ok, [], slave_ok, mongos?}
      else
        with {:ok, connection} <-
               servers
               |> Enum.random()
               |> get_connection(topology_pid) do
          {:ok, connection, slave_ok, mongos?}
        end
      end
    end
  end

  defp select_servers(topology_pid, type, opts) do
    start_time = System.monotonic_time()
    select_servers(topology_pid, type, opts, start_time)
  end

  @sel_timeout 30000
  # NOTE: Should think about the handling completely in the Topology GenServer
  #       in order to make the entire operation atomic instead of querying
  #       and then potentially having an outdated topology when waiting for the
  #       connection.
  defp select_servers(topology_pid, type, opts, start_time) do
    topology = Topology.topology(topology_pid)

    with {:ok, servers, slave_ok, mongos?} <-
           TopologyDescription.select_servers(topology, type, opts) do
      if Enum.empty?(servers) do
        case Topology.wait_for_connection(topology_pid, @sel_timeout, start_time) do
          {:ok, _servers} ->
            select_servers(topology_pid, type, opts, start_time)

          {:error, :selection_timeout} ->
            {:error,
             %Mongo.Error{type: :network, message: "Topology selection timeout", code: 89}}
        end
      else
        {:ok, servers, slave_ok, mongos?}
      end
    end
  end

  defp get_connection(server, pid) do
    if server != nil do
      with {:ok, connection} <- Topology.connection_for_address(pid, server) do
        {:ok, connection}
      end
    else
      {:ok, nil}
    end
  end

  defp modifier_docs([{key, _} | _], type),
    do: key |> key_to_string |> modifier_key(type)

  defp modifier_docs(map, _type) when is_map(map) and map_size(map) == 0,
    do: :ok

  defp modifier_docs(map, type) when is_map(map),
    do: Enum.at(map, 0) |> elem(0) |> key_to_string |> modifier_key(type)

  defp modifier_docs(list, type) when is_list(list),
    do: Enum.map(list, &modifier_docs(&1, type))

  defp modifier_key(<<?$, _::binary>> = other, :replace),
    do: raise(ArgumentError, "replace does not allow atomic modifiers, got: #{other}")

  defp modifier_key(<<?$, _::binary>>, :update),
    do: :ok

  defp modifier_key(<<_, _::binary>> = other, :update),
    do: raise(ArgumentError, "update only allows atomic modifiers, got: #{other}")

  defp modifier_key(_, _),
    do: :ok

  defp key_to_string(key) when is_atom(key),
    do: Atom.to_string(key)

  defp key_to_string(key) when is_binary(key),
    do: key

  defp cursor(conn, coll, query, select, opts) do
    %Mongo.Cursor{conn: conn, coll: coll, query: query, select: select, opts: opts}
  end

  defp aggregation_cursor(conn, coll, query, select, opts) do
    %Mongo.AggregationCursor{conn: conn, coll: coll, query: query, select: select, opts: opts}
  end

  defp filter_nils(keyword) when is_list(keyword) do
    Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
  end

  defp filter_nils(map) when is_map(map) do
    Enum.reject(map, fn {_key, value} -> is_nil(value) end)
    |> Enum.into(%{})
  end

  defp normalize_doc(doc) do
    Enum.reduce(doc, {:unknown, []}, fn
      {key, _value}, {:binary, _acc} when is_atom(key) ->
        invalid_doc(doc)

      {key, _value}, {:atom, _acc} when is_binary(key) ->
        invalid_doc(doc)

      {key, value}, {_, acc} when is_atom(key) ->
        {:atom, [{key, value} | acc]}

      {key, value}, {_, acc} when is_binary(key) ->
        {:binary, [{key, value} | acc]}
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp invalid_doc(doc) do
    message = "invalid document containing atom and string keys: #{inspect(doc)}"
    raise ArgumentError, message
  end

  defp cursor_type(nil),
    do: []

  defp cursor_type(:tailable),
    do: [tailable_cursor: true]

  defp cursor_type(:tailable_await),
    do: [tailable_cursor: true, await_data: true]

  defp assert_single_doc!(doc) when is_map(doc), do: :ok
  defp assert_single_doc!([]), do: :ok
  defp assert_single_doc!([{_, _} | _]), do: :ok

  defp assert_single_doc!([_] = doc), do: raise_not_single_doc(doc)

  defp assert_single_doc!(other) do
    unless Mongo.Encoder.impl_for(other),
      do: raise_not_single_doc(other),
      else: :ok
  end

  defp raise_not_single_doc(doc),
    do: raise(ArgumentError, "expected single document, got: #{inspect(doc)}")

  defp assert_many_docs!([first | _]) when not is_tuple(first), do: :ok

  defp assert_many_docs!(docs) when is_list(docs),
    do: Enum.all?(docs, &assert_single_doc!/1) && :ok

  defp assert_many_docs!(other) do
    raise ArgumentError, "expected list of documents, got: #{inspect(other)}"
  end

  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end

  defp assign_ids(list) when is_list(list) do
    list
    |> Enum.map(&Mongo.Encoder.encode/1)
    |> Enum.map(&assign_id/1)
    |> Enum.unzip()
  end

  defp assign_id(%{_id: id} = map) when id != nil,
    do: {id, map}

  defp assign_id(%{"_id" => id} = map) when id != nil,
    do: {id, map}

  defp assign_id([{_, _} | _] = keyword) do
    case Keyword.take(keyword, [:_id, "_id"]) do
      [{_key, id} | _] when id != nil ->
        {id, keyword}

      [] ->
        add_id(keyword)
    end
  end

  defp assign_id(map) when is_map(map) do
    map |> Map.to_list() |> add_id
  end

  defp add_id(doc) do
    id = Mongo.IdServer.new()
    {id, add_id(doc, id)}
  end

  defp add_id([{key, _} | _] = list, id) when is_atom(key) do
    [{:_id, id} | list]
  end

  defp add_id([{key, _} | _] = list, id) when is_binary(key) do
    [{"_id", id} | list]
  end

  defp add_id([], id) do
    # Why are you inserting empty documents =(
    [{"_id", id}]
  end

  defp maybe_failure(op_reply(flags: flags, docs: [%{"$err" => reason, "code" => code}]))
       when (@reply_query_failure &&& flags) != 0,
       do: {:error, Mongo.Error.exception(message: reason, code: code)}

  defp maybe_failure(op_reply(flags: flags))
       when (@reply_cursor_not_found &&& flags) != 0,
       do: {:error, Mongo.Error.exception(message: "cursor not found")}

  defp maybe_failure(_reply),
    do: :ok
end
