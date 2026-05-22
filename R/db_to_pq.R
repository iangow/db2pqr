#' Export a PostgreSQL table to Parquet
#'
#' Exports a table from PostgreSQL to a Parquet file, with optional row
#' filtering, column filtering, column renaming, Arrow type overrides, and
#' timestamp normalization.
#'
#' @param table_name Name of the source PostgreSQL table.
#' @param schema Name of the source PostgreSQL schema.
#' @param host,database,user,password,port PostgreSQL connection parameters
#'   used when `con` is not supplied.
#' @param data_dir Root directory of the local Parquet data repository. Defaults
#'   to \code{\link{db2pq_data_dir}}.
#' @param out_file Optional full output path. Overrides `data_dir`, `schema`,
#'   and `table_name`.
#' @param where Optional SQL `WHERE` clause without the `WHERE` keyword.
#' @param obs Optional integer row limit.
#' @param keep,drop Optional character vectors of regex patterns used to select
#'   source columns. `drop` is applied before `keep`.
#' @param rename Optional named character vector or list mapping source column
#'   names to output column names, e.g. `c(conm = "company_name")`.
#' @param alt_table_name Optional output basename when `out_file` is omitted.
#' @param chunk_size Number of rows fetched and written per chunk.
#' @param transfer_method Transfer backend: `"dbi"` for the stable DBI path or
#'   `"adbc"` for the optional Arrow/ADBC path.
#' @param numeric_mode Numeric handling mode for PostgreSQL `numeric` columns.
#'   The default `"decimal"` transfers them as text and writes bounded
#'   `numeric(p, s)` columns as Arrow decimal Parquet columns. `"float64"` casts
#'   them to `DOUBLE PRECISION` before transfer. `"text"` transfers them as text
#'   without recreating decimal types. `"raw"` keeps the backend default.
#' @param con Optional existing DBI connection.
#' @param metadata Optional named list of Parquet schema metadata.
#' @param col_types Optional named list of Arrow output type overrides. Names
#'   refer to output column names after `rename`.
#' @param tz Time zone used to interpret `TIMESTAMP WITHOUT TIME ZONE` columns.
#'
#' @return Invisibly returns the output file path.
#' @export
db_to_pq <- function(
    table_name,
    schema,
    host = Sys.getenv("PGHOST", "localhost"),
    database = Sys.getenv("PGDATABASE", unset = Sys.getenv("PGUSER")),
    user = Sys.getenv("PGUSER", Sys.info()[["user"]]),
    password = Sys.getenv("PGPASSWORD", ""),
    port = as.integer(Sys.getenv("PGPORT", 5432)),
    data_dir = db2pq_data_dir(),
    out_file = NULL,
    where = NULL,
    obs = NULL,
    keep = NULL,
    drop = NULL,
    rename = NULL,
    alt_table_name = NULL,
    chunk_size = 100000L,
    transfer_method = c("dbi", "adbc"),
    numeric_mode = c("decimal", "float64", "text", "raw"),
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
    rename = rename,
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
                           keep = NULL, drop = NULL, rename = NULL,
                           col_types = NULL,
                           tz = NULL, transfer_method = c("dbi", "adbc"),
                           numeric_mode = c("decimal", "float64", "text", "raw")) {
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
  output_names <- .resolve_rename(nms, rename)

  # Resolve col_types up front so we can inspect types during SQL building
  resolved_col_types <- if (!is.null(col_types)) lapply(col_types, arrow_type) else list()
  numeric_plan <- .numeric_table_export_plan(
    con = con,
    schema = schema,
    table_name = table_name,
    columns = nms,
    output_names = output_names,
    col_types = resolved_col_types,
    transfer_method = transfer_method,
    numeric_mode = numeric_mode
  )
  resolved_col_types <- numeric_plan$col_types
  numeric_double_cols <- numeric_plan$numeric_double_cols
  numeric_text_cols <- numeric_plan$numeric_text_cols

  # Build SELECT expressions, wrapping timestamp columns with AT TIME ZONE
  if (!is.null(tz)) {
    naive_ts <- .naive_timestamp_cols(con, schema, table_name, nms)

    # Columns requested as timestamp via col_types but not natively timestamp in PG
    timestamp_type_names <- names(resolved_col_types)[
      vapply(resolved_col_types, .is_arrow_timestamp, logical(1))
    ]
    cast_ts <- nms[output_names %in% timestamp_type_names & !(nms %in% naive_ts)]

    ts_cols <- union(naive_ts, cast_ts)
    if (length(ts_cols) > 0) {
      message("Applying tz='", tz, "' to ", length(ts_cols),
              " timestamp column(s): ", paste(ts_cols, collapse = ", "), ".")
    }

    timestamp_exprs <- stats::setNames(vapply(nms, function(col) {
      out_col <- output_names[[match(col, nms)]]
      if (col %in% naive_ts) {
        return(sprintf('("%s" AT TIME ZONE \'%s\') AS "%s"', col, tz, out_col))
      } else if (col %in% cast_ts) {
        return(sprintf('(CAST("%s" AS TIMESTAMP) AT TIME ZONE \'%s\') AS "%s"', col, tz, out_col))
      }
      ""
    }, character(1)), nms)
    timestamp_exprs <- timestamp_exprs[nzchar(timestamp_exprs)]

    col_exprs <- .rewrite_select_list(
      columns = nms,
      output_names = output_names,
      numeric_cast_cols = numeric_double_cols,
      numeric_text_cols = numeric_text_cols,
      timestamp_exprs = timestamp_exprs
    )

    # Remove cast_ts cols from col_types - PG now returns them as timestamptz
    resolved_col_types <- resolved_col_types[
      setdiff(names(resolved_col_types), output_names[nms %in% cast_ts])
    ]
  } else {
    col_exprs <- .rewrite_select_list(
      columns = nms,
      output_names = output_names,
      numeric_cast_cols = numeric_double_cols,
      numeric_text_cols = numeric_text_cols
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
    output_names = output_names,
    col_types = col_types_final
  )
}

.resolve_rename <- function(columns, rename = NULL) {
  output_names <- columns
  if (is.null(rename)) {
    return(output_names)
  }

  rename <- unlist(rename, use.names = TRUE)
  if (is.null(names(rename)) || any(!nzchar(names(rename))) || any(!nzchar(rename))) {
    stop("`rename` must be a named character vector or list mapping source names to output names.")
  }

  unknown <- setdiff(names(rename), columns)
  if (length(unknown) > 0L) {
    stop("`rename` contains source column(s) not selected for export: ",
         paste(unknown, collapse = ", "), call. = FALSE)
  }

  output_names[match(names(rename), columns)] <- as.character(rename)
  if (anyDuplicated(output_names)) {
    stop("`rename` must not produce duplicate output column names.", call. = FALSE)
  }

  output_names
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

.numeric_column_info <- function(con, schema, table_name, nms) {
  rows <- DBI::dbGetQuery(con,
    "SELECT column_name, numeric_precision, numeric_scale
     FROM information_schema.columns
     WHERE table_schema = $1
       AND table_name   = $2
       AND data_type    = 'numeric'",
    params = list(schema, table_name)
  )

  if (nrow(rows) == 0L) {
    return(data.frame(
      column_name = character(),
      numeric_precision = integer(),
      numeric_scale = integer()
    ))
  }

  for (col in c("numeric_precision", "numeric_scale")) {
    if (!col %in% names(rows)) {
      rows[[col]] <- NA_integer_
    }
  }

  rows[rows$column_name %in% nms, , drop = FALSE]
}

.numeric_table_export_plan <- function(con, schema, table_name, columns,
                                       output_names = columns,
                                       col_types = list(),
                                       transfer_method = c("dbi", "adbc"),
                                       numeric_mode = c("decimal", "float64", "text", "raw")) {
  transfer_method <- match.arg(transfer_method)
  numeric_mode <- match.arg(numeric_mode)

  if (identical(numeric_mode, "raw")) {
    return(list(
      col_types = col_types,
      numeric_double_cols = character(),
      numeric_text_cols = character()
    ))
  }

  numeric_info <- .numeric_column_info(con, schema, table_name, columns)
  typed_source_cols <- columns[output_names %in% names(col_types)]
  numeric_cols <- setdiff(numeric_info$column_name, typed_source_cols)

  if (identical(numeric_mode, "float64")) {
    if (identical(transfer_method, "adbc") && length(numeric_cols) > 0L) {
      message(
        "Casting ", length(numeric_cols),
        " PostgreSQL numeric column(s) to DOUBLE PRECISION for ADBC: ",
        paste(numeric_cols, collapse = ", "), "."
      )
    }
    return(list(
      col_types = col_types,
      numeric_double_cols = numeric_cols,
      numeric_text_cols = character()
    ))
  }

  if (identical(numeric_mode, "decimal")) {
    col_types <- utils::modifyList(
      .pg_decimal_col_types(
        numeric_info = numeric_info,
        columns = columns,
        output_names = output_names,
        explicit_col_types = col_types
      ),
      col_types
    )
  }

  list(
    col_types = col_types,
    numeric_double_cols = character(),
    numeric_text_cols = numeric_cols
  )
}

.pg_decimal_col_types <- function(numeric_info, columns, output_names = columns,
                                  explicit_col_types = list()) {
  out <- list()
  output_names <- stats::setNames(output_names, columns)

  for (i in seq_len(nrow(numeric_info))) {
    source_name <- numeric_info$column_name[[i]]
    output_name <- output_names[[source_name]]
    precision <- numeric_info$numeric_precision[[i]]
    scale <- numeric_info$numeric_scale[[i]]

    if (output_name %in% names(explicit_col_types) ||
        is.na(precision) || is.na(scale) ||
        precision < 1L || precision > 76L) {
      next
    }

    out[[output_name]] <- arrow::decimal(
      precision = as.integer(precision),
      scale = as.integer(scale)
    )
  }

  out
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

.rewrite_select_list <- function(columns, output_names = columns,
                                 numeric_cast_cols = character(),
                                 numeric_text_cols = character(),
                                 timestamp_exprs = NULL) {
  output_names <- stats::setNames(output_names, columns)
  unname(vapply(columns, function(col) {
    out_col <- output_names[[col]]
    if (!is.null(timestamp_exprs) && col %in% names(timestamp_exprs)) {
      return(timestamp_exprs[[col]])
    }
    if (col %in% numeric_cast_cols) {
      return(sprintf('CAST("%s" AS DOUBLE PRECISION) AS "%s"', col, out_col))
    }
    if (col %in% numeric_text_cols) {
      return(sprintf('CAST("%s" AS TEXT) AS "%s"', col, out_col))
    }
    if (!identical(col, out_col)) {
      return(sprintf('"%s" AS "%s"', col, out_col))
    }
    sprintf('"%s"', col)
  }, character(1)))
}
