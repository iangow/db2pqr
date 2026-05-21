#' Export all tables in a WRDS schema to Parquet
#'
#' Iterates over tables in a WRDS PostgreSQL schema and calls
#' \code{\link{wrds_update_pq}} for each table.
#'
#' @param schema WRDS schema name.
#' @param data_dir Root directory of the local Parquet data repository.
#' @param force If `TRUE`, re-download tables even if local files appear
#'   current.
#' @param tables Optional subset of table names to process.
#' @param chunk_size Number of rows fetched and written per chunk.
#' @param wrds_id Optional WRDS username override.
#' @param transfer_method Transfer backend passed to
#'   \code{\link{wrds_update_pq}}.
#' @param numeric_mode Numeric handling mode passed to
#'   \code{\link{wrds_update_pq}}.
#' @param ... Additional arguments passed to \code{\link{wrds_update_pq}}.
#'
#' @return Invisibly returns a named list of output paths or `NULL` values for
#'   skipped/failed tables.
#' @export
wrds_schema_to_pq <- function(
    schema,
    data_dir = Sys.getenv("DATA_DIR", "."),
    force = FALSE,
    tables = NULL,
    chunk_size = 100000L,
    wrds_id = NULL,
    transfer_method = c("dbi", "adbc"),
    numeric_mode = c("float64", "raw"),
    ...) {
  transfer_method <- match.arg(transfer_method)
  numeric_mode <- match.arg(numeric_mode)

  all_tables <- wrds_get_tables(schema, wrds_id = wrds_id)

  if (!is.null(tables)) {
    unknown <- setdiff(tables, all_tables)
    if (length(unknown) > 0) {
      warning(
        "The following tables were not found in schema '", schema, "' and will be skipped: ",
        paste(unknown, collapse = ", ")
      )
    }
    all_tables <- intersect(tables, all_tables)
  }

  if (length(all_tables) == 0) {
    message("No tables to process in schema '", schema, "'.")
    return(invisible(character(0)))
  }

  message("Processing ", length(all_tables), " table(s) in schema '", schema, "'.")

  out_files <- vector("list", length(all_tables))
  names(out_files) <- all_tables

  for (table_name in all_tables) {
    out_files[[table_name]] <- tryCatch(
      wrds_update_pq(
        table_name = table_name,
        schema     = schema,
        data_dir   = data_dir,
        force      = force,
        chunk_size = chunk_size,
        wrds_id = wrds_id,
        transfer_method = transfer_method,
        numeric_mode = numeric_mode,
        ...
      ),
      error = function(e) {
        message("Error processing '", schema, ".", table_name, "': ", conditionMessage(e))
        NULL
      }
    )
  }

  invisible(out_files)
}
