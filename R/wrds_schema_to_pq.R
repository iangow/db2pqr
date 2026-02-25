wrds_schema_to_pq <- function(
    schema,
    data_dir = Sys.getenv("DATA_DIR", "."),
    force = FALSE,
    tables = NULL,
    chunk_size = 100000L) {

  con <- wrds::wrds_connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  all_tables <- wrds::list_tables(con, schema)

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
        chunk_size = chunk_size
      ),
      error = function(e) {
        message("Error processing '", schema, ".", table_name, "': ", conditionMessage(e))
        NULL
      }
    )
  }

  invisible(out_files)
}
