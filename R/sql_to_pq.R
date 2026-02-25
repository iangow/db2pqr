sql_to_pq <- function(con, sql, out_file, chunk_size = 100000, metadata = NULL,
                      col_types = NULL) {
  if (!is.null(col_types)) {
    if (!is.list(col_types) || is.null(names(col_types))) {
      stop("`col_types` must be a named list, ",
           "e.g. list(permno = \"int32\") or list(permno = arrow::int32()).")
    }
    col_types <- lapply(col_types, arrow_type)
  }

  res  <- DBI::dbSendQuery(con, sql)
  sink <- arrow::FileOutputStream$create(out_file)
  writer <- NULL

  n_total <- 0L

  tryCatch({
    repeat {
      chunk <- DBI::dbFetch(res, n = chunk_size)
      if (nrow(chunk) == 0) break
      n_total <- n_total + nrow(chunk)

      tab <- arrow::Table$create(chunk)

      # Apply column type casts
      if (!is.null(col_types)) {
        for (col in intersect(names(col_types), tab$schema$names)) {
          idx <- match(col, tab$schema$names) - 1L  # 0-indexed
          tab <- tab$SetColumn(idx, arrow::field(col, col_types[[col]]),
                               tab$column(idx)$cast(col_types[[col]]))
        }
      }

      if (is.null(writer)) {
        schema <- tab$schema
        if (!is.null(metadata)) {
          existing <- if (is.null(schema$metadata)) list() else schema$metadata
          schema <- schema$WithMetadata(c(existing, metadata))
        }
        props <- arrow::ParquetWriterProperties$create(
          column_names = schema$names
        )
        writer <- arrow::ParquetFileWriter$create(
          schema = schema,
          sink = sink,
          properties = props
        )
      }

      writer$WriteTable(tab, chunk_size = chunk_size)
    }

    # If we never wrote anything, don't leave a broken file behind
    if (n_total == 0L) {
      stop("Query returned 0 rows; no Parquet file written.")
    }

  }, finally = {
    # Close writer first (writes footer), then sink, then DBI result
    if (!is.null(writer)) {
      try(writer$Close(), silent = TRUE)
    }
    try(sink$close(), silent = TRUE)
    try(DBI::dbClearResult(res), silent = TRUE)
  })

  invisible(out_file)
}
