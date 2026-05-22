#' List PostgreSQL relations in a schema
#'
#' @param schema PostgreSQL schema name.
#' @param views If `TRUE`, include views as well as base tables.
#' @param con Optional existing DBI connection.
#' @param host,database,user,password,port PostgreSQL connection parameters
#'   used when `con` is not supplied.
#'
#' @return A character vector of relation names.
#' @export
db_schema_tables <- function(schema, views = FALSE, con = NULL,
                             host = Sys.getenv("PGHOST", "localhost"),
                             database = Sys.getenv("PGDATABASE", unset = Sys.getenv("PGUSER")),
                             user = Sys.getenv("PGUSER", Sys.info()[["user"]]),
                             password = Sys.getenv("PGPASSWORD", ""),
                             port = as.integer(Sys.getenv("PGPORT", 5432))) {
  if (is.null(con)) {
    con <- DBI::dbConnect(
      RPostgres::Postgres(),
      host = host,
      dbname = database,
      user = user,
      password = password,
      port = port
    )
    on.exit(DBI::dbDisconnect(con), add = TRUE)
  }

  table_types <- if (isTRUE(views)) {
    c("BASE TABLE", "VIEW")
  } else {
    "BASE TABLE"
  }

  rows <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT table_name ",
      "FROM information_schema.tables ",
      "WHERE table_schema = $1 ",
      "AND table_type IN (", paste0("$", seq_along(table_types) + 1L, collapse = ", "), ") ",
      "ORDER BY table_name"
    ),
    params = c(list(schema), as.list(table_types))
  )

  rows$table_name
}

#' List WRDS relations in a schema
#'
#' @param schema WRDS schema name.
#' @param wrds_id Optional WRDS username override.
#' @param views If `TRUE`, include views as well as base tables.
#'
#' @return A character vector of relation names.
#' @export
wrds_get_tables <- function(schema, wrds_id = NULL, views = FALSE) {
  con <- .wrds_connect_dbi(wrds_id = wrds_id)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  db_schema_tables(schema = schema, views = views, con = con)
}

#' Export all tables in a PostgreSQL schema to Parquet
#'
#' @inheritParams db_schema_tables
#' @param data_dir Root directory of the local Parquet data repository.
#' @param force If `FALSE`, existing output files are skipped.
#' @param tables Optional subset of table names to export.
#' @param chunk_size Number of rows fetched and written per chunk.
#' @param transfer_method Transfer backend passed to \code{\link{db_to_pq}}.
#' @param numeric_mode Numeric handling mode passed to \code{\link{db_to_pq}}.
#' @param archive Whether existing output files should be archived before
#'   replacement.
#' @param archive_dir Archive directory name.
#' @param ... Additional arguments passed to \code{\link{db_to_pq}}.
#'
#' @return Invisibly returns a named list of output paths.
#' @export
db_schema_to_pq <- function(schema, data_dir = Sys.getenv("DATA_DIR", "."),
                            force = FALSE, tables = NULL, chunk_size = 100000L,
                            transfer_method = c("dbi", "adbc"),
                            numeric_mode = c("decimal", "float64", "text", "raw"),
                            archive = FALSE, archive_dir = "archive",
                            con = NULL, ...) {
  transfer_method <- match.arg(transfer_method)
  numeric_mode <- match.arg(numeric_mode)

  own_con <- is.null(con)
  if (own_con) {
    con <- DBI::dbConnect(RPostgres::Postgres())
    on.exit(DBI::dbDisconnect(con), add = TRUE)
  }

  all_tables <- db_schema_tables(schema = schema, con = con)
  if (!is.null(tables)) {
    unknown <- setdiff(tables, all_tables)
    if (length(unknown) > 0L) {
      warning(
        "The following tables were not found in schema '", schema, "' and will be skipped: ",
        paste(unknown, collapse = ", "),
        call. = FALSE
      )
    }
    all_tables <- intersect(tables, all_tables)
  }

  if (length(all_tables) == 0L) {
    message("No tables to process in schema '", schema, "'.")
    return(invisible(character()))
  }

  out_files <- vector("list", length(all_tables))
  names(out_files) <- all_tables

  for (table_name in all_tables) {
    out_file <- file.path(data_dir, schema, paste0(table_name, ".parquet"))
    if (!force && file.exists(out_file)) {
      out_files[[table_name]] <- out_file
      next
    }
    if (archive && file.exists(out_file)) {
      pq_archive(file_name = out_file, archive_dir = archive_dir)
    }
    out_files[[table_name]] <- tryCatch(
      db_to_pq(
        table_name = table_name,
        schema = schema,
        data_dir = data_dir,
        chunk_size = chunk_size,
        transfer_method = transfer_method,
        numeric_mode = numeric_mode,
        con = con,
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
