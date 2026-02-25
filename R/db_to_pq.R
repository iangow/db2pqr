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
    chunk_size = 100000L) {

  # Build output path: <data_dir>/<schema>/<table>.parquet
  out_name <- if (!is.null(alt_table_name)) alt_table_name else table_name
  if (is.null(out_file)) {
    out_file <- file.path(data_dir, schema, paste0(out_name, ".parquet"))
  }
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)

  # Connect first so we can resolve column names if keep/drop are specified
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host     = host,
    dbname   = database,
    user     = user,
    password = password,
    port     = port
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Resolve column list: apply drop then keep (OR logic within each)
  col_select <- "*"
  if (!is.null(keep) || !is.null(drop)) {
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

    col_select <- paste(sprintf('"%s"', nms), collapse = ", ")
  }

  # Build SQL query
  tbl_ref <- sprintf('"%s"."%s"', schema, table_name)
  sql <- paste("SELECT", col_select, "FROM", tbl_ref)
  if (!is.null(where)) {
    sql <- paste(sql, "WHERE", where)
  }
  if (!is.null(obs)) {
    sql <- paste(sql, "LIMIT", as.integer(obs))
  }

  sql_to_pq(con, sql, out_file, chunk_size = chunk_size)
}
