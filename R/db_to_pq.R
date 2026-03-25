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
    transfer_method = c("dbi", "adbc"),
    numeric_mode = c("float64", "raw"),
    con = NULL,
    metadata = NULL,
    col_types = NULL,
    tz = NULL) {
  transfer_method <- match.arg(transfer_method)
  numeric_mode <- match.arg(numeric_mode)

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

  plan <- .db_to_pq_plan(
    con = con,
    table_name = table_name,
    schema = schema,
    where = where,
    obs = obs,
    keep = keep,
    drop = drop,
    col_types = col_types,
    tz = tz,
    transfer_method = transfer_method,
    numeric_mode = numeric_mode
  )

  if (identical(transfer_method, "dbi")) {
    sql_to_pq(
      con = con,
      sql = plan$sql,
      out_file = out_file,
      chunk_size = chunk_size,
      metadata = metadata,
      col_types = plan$col_types
    )
  } else {
    if (.is_adbi_connection(con)) {
      sql_to_pq_adbc(
        con = con,
        sql = plan$sql,
        out_file = out_file,
        chunk_size = chunk_size,
        metadata = metadata,
        col_types = plan$col_types
      )
    } else {
      sql_to_pq_dev(
        con = con,
        sql = plan$sql,
        out_file = out_file,
        chunk_size = chunk_size,
        metadata = metadata,
        col_types = plan$col_types
      )
    }
  }

  invisible(out_file)
}

.db_to_pq_plan <- function(con, table_name, schema, where = NULL, obs = NULL,
                           keep = NULL, drop = NULL, col_types = NULL,
                           tz = NULL, transfer_method = c("dbi", "adbc"),
                           numeric_mode = c("float64", "raw")) {
  transfer_method <- match.arg(transfer_method)
  numeric_mode <- match.arg(numeric_mode)
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

  # Resolve col_types up front so we can inspect types during SQL building
  resolved_col_types <- if (!is.null(col_types)) lapply(col_types, arrow_type) else list()
  adbc_plan <- .adbc_table_export_plan(
    con = con,
    schema = schema,
    table_name = table_name,
    columns = nms,
    col_types = resolved_col_types,
    transfer_method = transfer_method,
    numeric_mode = numeric_mode
  )
  resolved_col_types <- adbc_plan$col_types
  numeric_cast_cols <- adbc_plan$numeric_cast_cols

  # Build SELECT expressions, wrapping timestamp columns with AT TIME ZONE
  if (!is.null(tz)) {
    naive_ts <- .naive_timestamp_cols(con, schema, table_name, nms)

    # Columns requested as timestamp via col_types but not natively timestamp in PG
    cast_ts <- intersect(
      names(resolved_col_types)[vapply(resolved_col_types, .is_arrow_timestamp, logical(1))],
      setdiff(nms, naive_ts)
    )

    ts_cols <- union(naive_ts, cast_ts)
    if (length(ts_cols) > 0) {
      message("Applying tz='", tz, "' to ", length(ts_cols),
              " timestamp column(s): ", paste(ts_cols, collapse = ", "), ".")
    }

    timestamp_exprs <- stats::setNames(vapply(nms, function(col) {
      if (col %in% naive_ts) {
        return(sprintf('("%s" AT TIME ZONE \'%s\') AS "%s"', col, tz, col))
      } else if (col %in% cast_ts) {
        return(sprintf('(CAST("%s" AS TIMESTAMP) AT TIME ZONE \'%s\') AS "%s"', col, tz, col))
      }
      ""
    }, character(1)), nms)
    timestamp_exprs <- timestamp_exprs[nzchar(timestamp_exprs)]

    col_exprs <- .rewrite_select_list(
      columns = nms,
      numeric_cast_cols = numeric_cast_cols,
      timestamp_exprs = timestamp_exprs
    )

    # Remove cast_ts cols from col_types - PG now returns them as timestamptz
    resolved_col_types <- resolved_col_types[setdiff(names(resolved_col_types), cast_ts)]
  } else {
    col_exprs <- .rewrite_select_list(
      columns = nms,
      numeric_cast_cols = numeric_cast_cols
    )
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

  col_types_final <- if (length(resolved_col_types) > 0) resolved_col_types else NULL

  list(
    sql = sql,
    columns = nms,
    col_types = col_types_final
  )
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

.numeric_cols <- function(con, schema, table_name, nms) {
  rows <- DBI::dbGetQuery(con,
    "SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = $1
       AND table_name   = $2
       AND data_type    = 'numeric'",
    params = list(schema, table_name)
  )

  intersect(nms, rows$column_name)
}

.adbc_table_export_plan <- function(con, schema, table_name, columns,
                                    col_types = list(),
                                    transfer_method = c("dbi", "adbc"),
                                    numeric_mode = c("float64", "raw")) {
  transfer_method <- match.arg(transfer_method)
  numeric_mode <- match.arg(numeric_mode)

  if (!identical(transfer_method, "adbc")) {
    return(list(
      col_types = col_types,
      numeric_cast_cols = character()
    ))
  }

  numeric_cast_cols <- if (identical(numeric_mode, "float64")) {
    .adbc_numeric_cast_cols(
      con = con,
      schema = schema,
      table_name = table_name,
      columns = columns,
      col_types = col_types
    )
  } else {
    character()
  }

  list(
    col_types = col_types,
    numeric_cast_cols = numeric_cast_cols
  )
}

.adbc_numeric_cast_cols <- function(con, schema, table_name, columns,
                                    col_types = list()) {
  numeric_cast_cols <- setdiff(
    .numeric_cols(con, schema, table_name, columns),
    names(col_types)
  )

  if (length(numeric_cast_cols) > 0L) {
    message(
      "Casting ", length(numeric_cast_cols),
      " PostgreSQL numeric column(s) to DOUBLE PRECISION for ADBC: ",
      paste(numeric_cast_cols, collapse = ", "), "."
    )
  }

  numeric_cast_cols
}

.adbc_numeric_cast_map_from_column_info <- function(column_info) {
  if (nrow(column_info) == 0L || !"name" %in% names(column_info) || !"type" %in% names(column_info)) {
    return(character())
  }

  numeric_like <- tolower(as.character(column_info$type)) %in% c("string", "utf8", "large_utf8")
  out <- as.character(column_info$type[numeric_like])
  names(out) <- column_info$name[numeric_like]
  names(out)
}

.rewrite_select_list <- function(columns, numeric_cast_cols = character(),
                                 timestamp_exprs = NULL) {
  unname(vapply(columns, function(col) {
    if (!is.null(timestamp_exprs) && col %in% names(timestamp_exprs)) {
      return(timestamp_exprs[[col]])
    }
    if (col %in% numeric_cast_cols) {
      return(sprintf('CAST("%s" AS DOUBLE PRECISION) AS "%s"', col, col))
    }
    sprintf('"%s"', col)
  }, character(1)))
}
