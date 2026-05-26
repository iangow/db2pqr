#' Export a WRDS SQL query to Parquet
#'
#' Runs SQL against WRDS PostgreSQL and writes the result to a Parquet file
#' using the same DBI or ADBC transfer paths as
#' \code{\link{wrds_update_pq}}.
#'
#' @param sql SQL query to execute against WRDS PostgreSQL.
#' @param table_name Logical table name used for the output basename.
#' @param schema Logical schema name used for the output directory.
#' @param wrds_id Optional WRDS username override.
#' @param data_dir Root directory of the local Parquet data repository.
#' @param out_file Optional full output path.
#' @param modified Optional last-modified metadata string to embed.
#' @param alt_table_name Optional output basename when `out_file` is omitted.
#' @param chunk_size Number of rows fetched and written per chunk.
#' @param transfer_method Transfer backend: `"dbi"` or `"adbc"`.
#' @param archive Whether an existing output file should be archived before
#'   replacement.
#' @param archive_dir Archive directory name.
#' @param col_types Optional named list of Arrow output type overrides.
#'
#' @return Invisibly returns the output file path.
#' @export
wrds_sql_to_pq <- function(sql, table_name, schema, wrds_id = NULL,
                           data_dir = NULL,
                           out_file = NULL, modified = NULL,
                           alt_table_name = NULL, chunk_size = NULL,
                           transfer_method = c("dbi", "adbc"),
                           archive = FALSE, archive_dir = "archive",
                           col_types = NULL) {
  transfer_method <- match.arg(transfer_method)
  chunk_size <- .resolve_wrds_chunk_size(chunk_size, transfer_method)
  data_dir <- pq_data_dir(data_dir)

  out_name <- if (!is.null(alt_table_name)) alt_table_name else table_name
  if (is.null(out_file)) {
    out_file <- file.path(data_dir, schema, paste0(out_name, ".parquet"))
  }
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)

  con <- if (identical(transfer_method, "adbc")) {
    wrds_connect_adbc(wrds_id = wrds_id)
  } else {
    .wrds_connect_dbi(wrds_id = wrds_id)
  }
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (archive && file.exists(out_file)) {
    pq_archive(file_name = out_file, archive_dir = archive_dir)
  }

  metadata <- if (!is.null(modified)) list(last_modified = modified) else NULL

  if (identical(transfer_method, "adbc")) {
    sql_to_pq_adbc(
      con = con,
      sql = sql,
      out_file = out_file,
      chunk_size = chunk_size,
      metadata = metadata,
      col_types = col_types
    )
  } else {
    sql_to_pq(
      con = con,
      sql = sql,
      out_file = out_file,
      chunk_size = chunk_size,
      metadata = metadata,
      col_types = col_types
    )
  }

  invisible(out_file)
}
