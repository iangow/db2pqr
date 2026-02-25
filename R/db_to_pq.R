db_to_pq <- function(
    table_name,
    schema,
    host = Sys.getenv("PGHOST", "localhost"),
    database = Sys.getenv("PGDATABASE", unset = Sys.getenv("PGUSER")),
    user = Sys.getenv("PGUSER", Sys.info()[["user"]]),
    password = Sys.getenv("PGPASSWORD", ""),
    port = as.integer(Sys.getenv("PGPORT", 5432)),
    data_dir = Sys.getenv("DATA_DIR", "."),
    out_file = NULL,
    where = NULL,
    obs = NULL,
    keep = NULL,
    drop = NULL,
    alt_table_name = NULL,
    chunk_size = 100000L,
    con = NULL,
    metadata = NULL,
    col_types = NULL,
    tz = NULL) {

  # Build output path: <data_dir>/<schema>/<table>.parquet
  out_name <- if (!is.null(alt_table_name)) alt_table_name else table_name
  if (is.null(out_file)) {
    out_file <- file.path(data_dir, schema, paste0(out_name, ".parquet"))
  }
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)

  # Open a connection if one wasn't supplied; close it on exit if we own it
  if (is.null(con)) {
    con <- DBI::dbConnect(
      RPostgres::Postgres(),
      host     = host,
      dbname   = database,
      user     = user,
      password = password,
      port     = port
    )
    on.exit(DBI::dbDisconnect(con), add = TRUE)
  }

  # Resolve column list, applying keep/drop filters
  nms <- DBI::dbListFields(con, DBI::Id(schema = schema, table = table_name))

  if (!is.null(drop)) {
    drop_match <- Reduce(`|`, lapply(as.character(drop), grepl, x = nms, perl = TRUE))
    nms <- nms[!drop_match]
  }
  if (!is.null(keep)) {
    keep_match <- Reduce(`|`, lapply(as.character(keep), grepl, x = nms, perl = TRUE))
    nms <- nms[keep_match]
  }
  if (length(nms) == 0) {
    stop("No columns selected after applying keep/drop filters.")
  }

  # Build SELECT expressions, wrapping naive timestamp columns with AT TIME ZONE
  if (!is.null(tz)) {
    naive_ts <- .naive_timestamp_cols(con, schema, table_name, nms)
    if (length(naive_ts) > 0) {
      message("Applying tz='", tz, "' to ", length(naive_ts),
              " timestamp without time zone column(s): ",
              paste(naive_ts, collapse = ", "), ".")
    }
    col_exprs <- ifelse(
      nms %in% naive_ts,
      sprintf('("%s" AT TIME ZONE \'%s\')', nms, tz),
      sprintf('"%s"', nms)
    )
  } else {
    col_exprs <- sprintf('"%s"', nms)
  }

  col_select <- paste(col_exprs, collapse = ", ")

  # Build SQL query
  tbl_ref <- sprintf('"%s"."%s"', schema, table_name)
  sql <- paste("SELECT", col_select, "FROM", tbl_ref)
  if (!is.null(where)) {
    sql <- paste(sql, "WHERE", where)
  }
  if (!is.null(obs)) {
    sql <- paste(sql, "LIMIT", as.integer(obs))
  }

  sql_to_pq(con, sql, out_file, chunk_size = chunk_size, metadata = metadata,
            col_types = col_types)
}

# Return the names of columns in `nms` whose PostgreSQL type is
# "timestamp without time zone".
.naive_timestamp_cols <- function(con, schema, table_name, nms) {
  rows <- DBI::dbGetQuery(con,
    "SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = $1
       AND table_name   = $2
       AND data_type    = 'timestamp without time zone'",
    params = list(schema, table_name)
  )
  intersect(nms, rows$column_name)
}
