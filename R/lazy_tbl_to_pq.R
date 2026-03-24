#' Export a lazy `dbplyr` table to Parquet
#'
#' Renders a lazy `dbplyr` query to SQL and streams the result to a Parquet file
#' using the package's internal SQL-to-Parquet writer. This avoids collecting
#' the full result into memory before writing.
#'
#' @param tbl A lazy table backed by `dbplyr`, such as the result of
#'   `dplyr::tbl()` or a pipeline of `dplyr` verbs on a remote table.
#' @param out_file Full path to the output Parquet file.
#' @param chunk_size Number of rows fetched and written per chunk. Default is
#'   `100000`.
#' @param metadata Optional named list of schema metadata to embed in the
#'   Parquet file.
#' @param col_types Optional named list specifying Arrow type overrides. Values
#'   may be string type names (for example `"int32"` or `"date"`) or Arrow
#'   `DataType` objects.
#'
#' @return Invisibly returns `out_file`.
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(RPostgres::Postgres())
#' qry <- dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsi")) |>
#'   dplyr::filter(date >= as.Date("2020-01-01"))
#'
#' lazy_tbl_to_pq(qry, "~/pq_data/crsp/dsi_recent.parquet")
#' }
#' @export
lazy_tbl_to_pq <- function(tbl, out_file, chunk_size = 100000L, metadata = NULL,
                           col_types = NULL) {
  if (!requireNamespace("dbplyr", quietly = TRUE)) {
    stop("Package `dbplyr` must be installed to use `lazy_tbl_to_pq()`.")
  }

  sql <- dbplyr::sql_render(tbl)
  con <- dbplyr::remote_con(tbl)
  resolved_col_types <- if (!is.null(col_types)) lapply(col_types, arrow_type) else NULL

  if (inherits(con, "PqConnection") && !is.null(resolved_col_types)) {
    pushdown <- .push_postgres_col_types(
      sql = as.character(sql),
      columns = colnames(tbl),
      col_types = resolved_col_types
    )
    sql <- pushdown$sql
    resolved_col_types <- pushdown$col_types
  }

  sql_to_pq(
    con = con,
    sql = as.character(sql),
    out_file = out_file,
    chunk_size = chunk_size,
    metadata = metadata,
    col_types = resolved_col_types
  )
}

.quote_sql_ident <- function(x) {
  paste0('"', gsub('"', '""', x, fixed = TRUE), '"')
}

.postgres_cast_type <- function(type) {
  switch(type$ToString(),
    "int16" = "smallint",
    "int32" = "integer",
    "int64" = "bigint",
    "float" = "real",
    "double" = "double precision",
    "bool" = "boolean",
    "string" = "text",
    "large_string" = "text",
    "date32[day]" = "date",
    "timestamp[us]" = "timestamp",
    "timestamp[us, tz=UTC]" = "timestamptz",
    NULL
  )
}

.push_postgres_col_types <- function(sql, columns, col_types) {
  pushdown_cols <- intersect(names(col_types), columns)
  if (length(pushdown_cols) == 0L) {
    return(list(sql = sql, col_types = col_types))
  }

  casts <- lapply(col_types[pushdown_cols], .postgres_cast_type)
  supported <- names(casts)[!vapply(casts, is.null, logical(1))]

  if (length(supported) == 0L) {
    return(list(sql = sql, col_types = col_types))
  }

  select_sql <- vapply(columns, function(col) {
    ident <- .quote_sql_ident(col)
    cast_type <- casts[[col]]
    if (is.null(cast_type)) {
      paste0("src.", ident)
    } else {
      paste0("CAST(src.", ident, " AS ", cast_type, ") AS ", ident)
    }
  }, character(1))

  list(
    sql = paste0(
      "SELECT ",
      paste(select_sql, collapse = ", "),
      "\nFROM (\n",
      sql,
      "\n) AS src"
    ),
    col_types = col_types[setdiff(names(col_types), supported)]
  )
}
