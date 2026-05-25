sql_to_pq <- function(con, sql, out_file, chunk_size = 100000, metadata = NULL,
                      col_types = NULL) {
  col_types <- .normalize_arrow_col_types(col_types)

  res  <- DBI::dbSendQuery(con, sql)
  sink <- arrow::FileOutputStream$create(out_file)
  writer <- NULL
  data_schema <- NULL

  n_total <- 0L

  tryCatch({
    repeat {
      chunk <- DBI::dbFetch(res, n = chunk_size)
      if (nrow(chunk) == 0) break
      n_total <- n_total + nrow(chunk)

      if (is.null(data_schema)) {
        tab <- arrow::Table$create(chunk)
        schemas <- .build_arrow_schemas(tab$schema, col_types = col_types, metadata = metadata)
        data_schema <- schemas$data_schema
        writer <- .create_parquet_writer_from_schema(schemas$writer_schema, sink)
        tab <- .coerce_arrow_table(tab, data_schema)
      } else {
        tab <- .coerce_arrow_table(arrow::Table$create(chunk), data_schema)
      }
      rm(chunk)

      writer$WriteTable(tab, chunk_size = chunk_size)
      rm(tab)
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

.normalize_arrow_col_types <- function(col_types) {
  if (is.null(col_types)) {
    return(NULL)
  }

  if (!is.list(col_types) || is.null(names(col_types))) {
    stop("`col_types` must be a named list, ",
         "e.g. list(permno = \"int32\") or list(permno = arrow::int32()).")
  }

  if (!all(vapply(col_types, inherits, logical(1), what = "DataType"))) {
    col_types <- lapply(col_types, arrow_type)
  }

  col_types
}

.cast_arrow_table <- function(tab, col_types) {
  if (is.null(col_types)) {
    return(tab)
  }

  .coerce_arrow_table(tab, .build_arrow_schemas(tab$schema, col_types = col_types)$data_schema)
}

.create_parquet_writer <- function(tab, sink, metadata = NULL) {
  writer_schema <- .build_arrow_schemas(tab$schema, metadata = metadata)$writer_schema
  .create_parquet_writer_from_schema(writer_schema, sink)
}

.coerce_arrow_table <- function(tab, schema) {
  if (is.null(schema) || tab$schema$Equals(schema, check_metadata = FALSE)) {
    return(tab)
  }

  tab$cast(schema)
}

.build_arrow_schemas <- function(schema, col_types = NULL, metadata = NULL) {
  fields <- lapply(schema$names, function(col) {
    idx <- match(col, schema$names)
    field <- schema$GetFieldByName(col)
    if (!is.null(col_types) && col %in% names(col_types)) {
      arrow::field(
        col,
        col_types[[col]],
        nullable = field$nullable
      )
    } else {
      field
    }
  })

  data_schema <- arrow::schema(fields)
  writer_schema <- data_schema
  if (!is.null(metadata)) {
    existing <- if (is.null(writer_schema$metadata)) list() else writer_schema$metadata
    writer_schema <- writer_schema$WithMetadata(c(existing, metadata))
  }

  list(
    data_schema = data_schema,
    writer_schema = writer_schema
  )
}

.create_parquet_writer_from_schema <- function(schema, sink) {
  props <- arrow::ParquetWriterProperties$create(
    column_names = schema$names
  )
  arrow::ParquetFileWriter$create(
    schema = schema,
    sink = sink,
    properties = props
  )
}
