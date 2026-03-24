#' Export a lazy `dbplyr` table to Parquet via ADBC
#'
#' Renders a lazy `dbplyr` query to SQL, derives an ADBC PostgreSQL connection
#' from the backing `RPostgres` connection, and streams Arrow batches directly
#' to a Parquet file.
#'
#' This is an experimental development path kept separate from
#' [lazy_tbl_to_pq()] while we evaluate the ADBC transfer route. The ADBC path
#' opens a second PostgreSQL connection for the transfer, so the database must
#' allow an additional concurrent connection beyond the one already backing
#' `tbl`.
#'
#' @param tbl A lazy table backed by `dbplyr`.
#' @param out_file Full path to the output Parquet file.
#' @param chunk_size Number of rows written per Parquet row group. Default is
#'   `100000`.
#' @param metadata Optional named list of schema metadata to embed in the
#'   Parquet file.
#' @param col_types Optional named list specifying Arrow type overrides. Values
#'   may be string type names (for example `"int32"` or `"date"`) or Arrow
#'   `DataType` objects.
#'
#' @return Invisibly returns `out_file`.
#' @export
tbl_to_pq <- function(tbl, out_file, chunk_size = 100000L, metadata = NULL,
                      col_types = NULL) {
  if (!requireNamespace("dbplyr", quietly = TRUE)) {
    stop("Package `dbplyr` must be installed to use `tbl_to_pq()`.")
  }

  sql <- dbplyr::sql_render(tbl)
  con <- dbplyr::remote_con(tbl)
  col_types <- .auto_numeric_col_types(tbl, col_types)

  sql_to_pq_dev(
    con = con,
    sql = as.character(sql),
    out_file = out_file,
    chunk_size = chunk_size,
    metadata = metadata,
    col_types = col_types
  )
}

#' Create an ADBC PostgreSQL connection from an `RPostgres` connection
#'
#' This helper is intentionally narrow: it only supports `RPostgres`
#' `PqConnection` objects and assumes enough connection information is available
#' to build a PostgreSQL URI. If any required piece is missing, the function
#' fails instead of attempting alternative fallbacks. The returned ADBC
#' connection is separate from the original `RPostgres` connection, so using
#' this helper requires capacity for an additional concurrent database
#' connection.
#'
#' @param con An `RPostgres` connection object.
#'
#' @return An ADBC connection created by `adbi`.
#' @export
con_to_adbi <- function(con) {
  if (!requireNamespace("adbi", quietly = TRUE) ||
      !requireNamespace("adbcpostgresql", quietly = TRUE)) {
    stop("Packages `adbi` and `adbcpostgresql` must be installed to use `con_to_adbi()`.")
  }

  uri <- .con_to_adbi_uri(con)
  DBI::dbConnect(adbi::adbi("adbcpostgresql"), uri = uri)
}

sql_to_pq_dev <- function(con, sql, out_file, chunk_size = 100000, metadata = NULL,
                          col_types = NULL) {
  col_types <- .normalize_arrow_col_types(col_types)

  adbi_con <- con_to_adbi(con)
  on.exit(DBI::dbDisconnect(adbi_con), add = TRUE)

  res <- DBI::dbSendQueryArrow(adbi_con, sql)
  sink <- arrow::FileOutputStream$create(out_file)
  writer <- NULL
  data_schema <- NULL
  n_total <- 0L

  tryCatch({
    repeat {
      batch <- DBI::dbFetchArrowChunk(res)
      if (.nanoarrow_num_rows(batch) == 0L) break
      tab <- arrow::as_arrow_table(batch)
      n_total <- n_total + tab$num_rows

      if (is.null(data_schema)) {
        schemas <- .build_arrow_schemas(tab$schema, col_types = col_types, metadata = metadata)
        data_schema <- schemas$data_schema
        writer <- .create_parquet_writer_from_schema(schemas$writer_schema, sink)
      }

      tab <- .coerce_arrow_table(tab, data_schema)
      writer$WriteTable(tab, chunk_size = chunk_size)
      rm(tab, batch)
    }

    if (n_total == 0L) {
      stop("Query returned 0 rows; no Parquet file written.")
    }
  }, finally = {
    if (!is.null(writer)) {
      try(writer$Close(), silent = TRUE)
    }
    try(sink$close(), silent = TRUE)
    try(DBI::dbClearResult(res), silent = TRUE)
  })

  invisible(out_file)
}

#' Debug the ADBC chunk fetch path for a lazy `dbplyr` table
#'
#' Fetches Arrow chunks one at a time using the experimental ADBC path and
#' records where failures occur. This is intended for diagnosing driver or
#' result-set issues without writing a Parquet file.
#'
#' @param tbl A lazy table backed by `dbplyr`.
#' @param max_chunks Maximum number of chunks to inspect. Default is `Inf`.
#'
#' @return A data frame with one row per attempted chunk.
#' @export
tbl_to_pq_debug <- function(tbl, max_chunks = Inf) {
  if (!requireNamespace("dbplyr", quietly = TRUE)) {
    stop("Package `dbplyr` must be installed to use `tbl_to_pq_debug()`.")
  }

  sql <- dbplyr::sql_render(tbl)
  con <- dbplyr::remote_con(tbl)

  sql_to_pq_dev_debug(
    con = con,
    sql = as.character(sql),
    max_chunks = max_chunks
  )
}

sql_to_pq_dev_debug <- function(con, sql, max_chunks = Inf) {
  adbi_con <- con_to_adbi(con)
  on.exit(DBI::dbDisconnect(adbi_con), add = TRUE)

  res <- DBI::dbSendQueryArrow(adbi_con, sql)
  on.exit(try(DBI::dbClearResult(res), silent = TRUE), add = TRUE)

  logs <- vector("list", 0)
  chunk_idx <- 0L

  repeat {
    if (chunk_idx >= max_chunks) {
      break
    }

    chunk_idx <- chunk_idx + 1L
    fetch_error <- NULL
    batch <- tryCatch(
      DBI::dbFetchArrowChunk(res),
      error = function(e) {
        fetch_error <<- conditionMessage(e)
        NULL
      }
    )

    if (!is.null(fetch_error)) {
      logs[[length(logs) + 1L]] <- data.frame(
        chunk = chunk_idx,
        rows = NA_integer_,
        fetch_ok = FALSE,
        table_ok = FALSE,
        schema = "",
        error_stage = "fetch",
        error = fetch_error,
        stringsAsFactors = FALSE
      )
      break
    }

    schema_txt <- .nanoarrow_schema_text(batch)
    batch_rows <- .nanoarrow_num_rows(batch)
    if (identical(batch_rows, 0L)) {
      logs[[length(logs) + 1L]] <- data.frame(
        chunk = chunk_idx,
        rows = 0L,
        fetch_ok = TRUE,
        table_ok = TRUE,
        schema = schema_txt,
        error_stage = "",
        error = "",
        stringsAsFactors = FALSE
      )
      break
    }
    table_error <- NULL
    tab <- tryCatch(
      arrow::as_arrow_table(batch),
      error = function(e) {
        table_error <<- conditionMessage(e)
        NULL
      }
    )

    if (!is.null(table_error)) {
      logs[[length(logs) + 1L]] <- data.frame(
        chunk = chunk_idx,
        rows = NA_integer_,
        fetch_ok = TRUE,
        table_ok = FALSE,
        schema = schema_txt,
        error_stage = "as_arrow_table",
        error = table_error,
        stringsAsFactors = FALSE
      )
      break
    }

    rows <- tab$num_rows
    logs[[length(logs) + 1L]] <- data.frame(
      chunk = chunk_idx,
      rows = rows,
      fetch_ok = TRUE,
      table_ok = TRUE,
      schema = schema_txt,
      error_stage = "",
      error = "",
      stringsAsFactors = FALSE
    )

    if (rows == 0L) {
      break
    }
  }

  do.call(rbind, logs)
}

.con_to_adbi_uri <- function(con) {
  if (!inherits(con, "PqConnection")) {
    stop("`con_to_adbi()` currently requires an `RPostgres` `PqConnection`.")
  }

  info <- tryCatch(DBI::dbGetInfo(con), error = function(e) NULL)
  if (is.null(info)) {
    stop("Could not read connection information from the supplied `RPostgres` connection.")
  }

  .adbi_uri_from_info(info)
}

.auto_numeric_col_types <- function(tbl, col_types = NULL) {
  inferred <- .infer_tbl_numeric_col_types(tbl)
  if (is.null(inferred)) {
    return(col_types)
  }

  if (is.null(col_types)) {
    return(inferred)
  }

  utils::modifyList(inferred, col_types)
}

.infer_tbl_numeric_col_types <- function(tbl) {
  con <- dbplyr::remote_con(tbl)
  if (!inherits(con, "PqConnection")) {
    return(NULL)
  }

  table_info <- .tbl_base_table_info(tbl)
  if (is.null(table_info)) {
    return(NULL)
  }

  .pg_numeric_col_types(
    con = con,
    schema = table_info$schema,
    table = table_info$table,
    cols = table_info$columns
  )
}

.tbl_base_table_info <- function(tbl) {
  lazy_query <- unclass(tbl)$lazy_query
  if (is.null(lazy_query)) {
    return(NULL)
  }

  base_query <- .tbl_base_query(lazy_query)
  table_path <- .tbl_base_query_path(base_query)
  if (is.null(base_query) || is.null(table_path)) {
    return(NULL)
  }

  path <- .parse_dbplyr_table_path(table_path)
  if (is.null(path)) {
    return(NULL)
  }

  cols <- intersect(colnames(tbl), base_query$vars %||% character())
  if (length(cols) == 0L) {
    return(NULL)
  }

  list(schema = path$schema, table = path$table, columns = cols)
}

.tbl_base_query_path <- function(base_query) {
  if (is.null(base_query)) {
    return(NULL)
  }
  if (!is.null(base_query$name)) {
    return(base_query$name)
  }
  if (!is.null(base_query$x) && inherits(base_query$x, "dbplyr_table_path")) {
    return(base_query$x)
  }
  NULL
}

.tbl_base_query <- function(lazy_query) {
  current <- lazy_query
  repeat {
    if (inherits(current, "lazy_base_query")) {
      return(current)
    }
    current <- current$x
    if (is.null(current)) {
      return(NULL)
    }
  }
}

.parse_dbplyr_table_path <- function(name) {
  parts <- strsplit(as.character(name), "\\.")[[1]]
  parts <- gsub('^"|"$', "", parts)
  if (length(parts) == 2L) {
    return(list(schema = parts[[1]], table = parts[[2]]))
  }
  if (length(parts) == 1L) {
    return(list(schema = "public", table = parts[[1]]))
  }
  NULL
}

.pg_numeric_col_types <- function(con, schema, table, cols) {
  cols <- as.character(cols)
  if (length(cols) == 0L) {
    return(NULL)
  }

  cols_sql <- paste(DBI::dbQuoteString(con, cols), collapse = ", ")
  sql <- paste0(
    "SELECT column_name, numeric_precision, numeric_scale ",
    "FROM information_schema.columns ",
    "WHERE table_schema = ", DBI::dbQuoteString(con, schema),
    " AND table_name = ", DBI::dbQuoteString(con, table),
    " AND data_type = 'numeric' ",
    " AND column_name IN (", cols_sql, ")"
  )

  info <- DBI::dbGetQuery(con, sql)
  if (nrow(info) == 0L) {
    return(NULL)
  }

  fallback_cols <- character()
  out <- list()

  for (i in seq_len(nrow(info))) {
    precision <- info$numeric_precision[[i]]
    scale <- info$numeric_scale[[i]]
    col <- info$column_name[[i]]

    if (!is.na(precision) && !is.na(scale) && precision <= 38L) {
      out[[col]] <- arrow::decimal128(as.integer(precision), as.integer(scale))
    } else {
      out[[col]] <- arrow::float64()
      fallback_cols <- c(fallback_cols, col)
    }
  }

  if (length(fallback_cols) > 0L) {
    message(
      "Falling back to float64 for PostgreSQL numeric column(s) with unsupported ",
      "or missing precision/scale: ", paste(fallback_cols, collapse = ", ")
    )
  }

  out
}

.adbi_uri_from_info <- function(info) {
  host <- .required_info(info, "host")
  dbname <- .required_info(info, "dbname")
  user <- .optional_info(info, "user")
  port <- .optional_info(info, "port")
  password <- Sys.getenv("PGPASSWORD", unset = "")

  if (startsWith(host, "/")) {
    .postgres_socket_uri(dbname = dbname, host = host, port = port,
                         user = user, password = password)
  } else {
    .postgres_host_uri(dbname = dbname, host = host, port = port,
                       user = user, password = password)
  }
}

.required_info <- function(info, field) {
  value <- .optional_info(info, field)
  if (!nzchar(value)) {
    stop("Connection info is missing required field `", field, "`.")
  }
  value
}

.optional_info <- function(info, field) {
  value <- info[[field]]
  if (is.null(value) || length(value) == 0L || is.na(value)) "" else as.character(value[[1]])
}

.postgres_uri_query <- function(user = "", password = "", host = "", port = "") {
  query <- c()

  if (nzchar(host)) {
    query <- c(query, paste0("host=", utils::URLencode(host, reserved = TRUE)))
  }
  if (nzchar(port)) {
    query <- c(query, paste0("port=", utils::URLencode(port, reserved = TRUE)))
  }
  if (nzchar(user)) {
    query <- c(query, paste0("user=", utils::URLencode(user, reserved = TRUE)))
  }
  if (nzchar(password)) {
    query <- c(query, paste0("password=", utils::URLencode(password, reserved = TRUE)))
  }

  query
}

.postgres_host_uri <- function(dbname, host, port = "", user = "", password = "") {
  query <- .postgres_uri_query(user = user, password = password)
  uri <- paste0(
    "postgresql://",
    host,
    if (nzchar(port)) paste0(":", port) else "",
    "/",
    utils::URLencode(dbname, reserved = TRUE)
  )

  if (length(query) > 0L) {
    uri <- paste0(uri, "?", paste(query, collapse = "&"))
  }

  uri
}

.postgres_socket_uri <- function(dbname, host, port = "", user = "", password = "") {
  query <- .postgres_uri_query(
    host = host,
    port = port,
    user = user,
    password = password
  )

  uri <- paste0(
    "postgresql:///",
    utils::URLencode(dbname, reserved = TRUE)
  )

  if (length(query) > 0L) {
    uri <- paste0(uri, "?", paste(query, collapse = "&"))
  }

  uri
}

.nanoarrow_schema_text <- function(batch) {
  schema <- tryCatch(
    nanoarrow::infer_nanoarrow_schema(batch),
    error = function(e) NULL
  )

  if (is.null(schema)) {
    return("")
  }

  paste(capture.output(print(schema)), collapse = "\n")
}

.nanoarrow_num_rows <- function(batch) {
  rows <- tryCatch(batch$length, error = function(e) NULL)
  if (is.null(rows) || length(rows) == 0L) {
    return(NA_integer_)
  }
  as.integer(rows[[1]])
}

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
