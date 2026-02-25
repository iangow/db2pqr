#' Get last-modified metadata for Parquet data files
#'
#' Retrieves the \code{last_modified} metadata embedded in Parquet files by
#' \code{\link{wrds_update_pq}}.
#'
#' Behaviour depends on the arguments supplied:
#'
#' \itemize{
#'   \item \strong{\code{table_name} provided, \code{archive = FALSE}} (default):
#'     Returns the raw \code{last_modified} string embedded in
#'     \code{<data_dir>/<schema>/<table_name>.parquet}, or \code{""} if the
#'     file has no such metadata.
#'   \item \strong{\code{table_name} provided, \code{archive = TRUE}}:
#'     Returns a data frame of all archived files matching \code{table_name}
#'     in \code{<data_dir>/<schema>/<archive_dir>/}.
#'   \item \strong{Only \code{schema} provided (no \code{table_name})}:
#'     Returns a data frame summarising all Parquet files in the schema
#'     directory (or its archive subdirectory if \code{archive = TRUE}).
#'   \item \strong{Neither \code{table_name} nor \code{schema} provided}:
#'     Returns a data frame summarising all Parquet files across every schema
#'     subdirectory of \code{data_dir}.
#' }
#'
#' @param table_name Optional. Name of a specific table.
#' @param schema Optional. Name of the schema (subdirectory under
#'   \code{data_dir}).
#' @param data_dir Root directory of the Parquet data repository. Defaults to
#'   the \code{DATA_DIR} environment variable, or \code{"."} if unset.
#' @param archive If \code{TRUE}, look in the archive subdirectory instead of
#'   the main schema directory.
#' @param archive_dir Name of the archive subdirectory. Defaults to
#'   \code{"archive"}.
#'
#' @return When \code{table_name} is provided and \code{archive = FALSE}, a
#'   single string. Otherwise a \link[tibble]{tibble} with columns
#'   \code{file_name}, \code{table}, \code{schema}, \code{last_mod} (a
#'   \code{POSIXct} in UTC), and \code{last_mod_str}.
#'
#' @examples
#' \dontrun{
#' # Raw metadata string for a single table
#' pq_last_modified("dsi", "crsp")
#'
#' # Summary of archived versions of a table
#' pq_last_modified("company", "comp", archive = TRUE)
#'
#' # Summary of all tables in a schema
#' pq_last_modified(schema = "crsp")
#'
#' # Summary of all tables across all schemas
#' pq_last_modified(data_dir = "data")
#' }
#'
#' @seealso \code{\link{wrds_update_pq}}
#' @export
pq_last_modified <- function(
    table_name = NULL,
    schema = NULL,
    data_dir = Sys.getenv("DATA_DIR", "."),
    archive = FALSE,
    archive_dir = "archive") {

  if (!is.null(table_name)) {
    if (is.null(schema)) stop("schema is required when table_name is provided.")

    if (!archive) {
      pq_file <- file.path(data_dir, schema, paste0(table_name, ".parquet"))
      return(.get_pq_raw(pq_file))
    }

    # archive = TRUE: scan archive dir for files belonging to this table
    scan_dir <- file.path(data_dir, schema, archive_dir)
    files <- list.files(scan_dir, pattern = "\\.parquet$", full.names = TRUE)
    rows <- lapply(files, function(f) {
      stem <- tools::file_path_sans_ext(basename(f))
      tbl  <- .restore_table_basename(stem)
      if (is.null(tbl) || tbl != table_name) return(NULL)
      .scan_row(f, stem, schema, archive = TRUE)
    })
    return(.rows_to_tibble(Filter(Negate(is.null), rows)))
  }

  # No table_name: scan directory / directories
  if (!is.null(schema)) {
    scan_dir <- if (archive) {
      file.path(data_dir, schema, archive_dir)
    } else {
      file.path(data_dir, schema)
    }
    rows <- .scan_dir(scan_dir, schema, archive = archive)
    return(.rows_to_tibble(rows))
  }

  # No table_name, no schema: scan all schema subdirs
  subdirs <- list.dirs(data_dir, recursive = FALSE, full.names = TRUE)
  rows <- unlist(lapply(subdirs, function(d) {
    schema_name <- basename(d)
    scan_dir <- if (archive) file.path(d, archive_dir) else d
    .scan_dir(scan_dir, schema_name, archive = archive)
  }), recursive = FALSE)
  .rows_to_tibble(rows)
}

# Read the raw last_modified string from a Parquet file's metadata.
# Uses open_dataset() to read only the footer, avoiding loading row data.
.get_pq_raw <- function(path) {
  if (!file.exists(path)) return("")
  meta <- tryCatch(
    arrow::open_dataset(path)$schema$metadata,
    error = function(e) NULL
  )
  val <- meta[["last_modified"]]
  if (is.null(val) || is.na(val)) "" else val
}

# Build a single summary row for a Parquet file.
.scan_row <- function(path, file_stem, schema_name, archive = FALSE) {
  raw <- .get_pq_raw(path)
  dttm <- .parse_wrds_datetime(raw)
  table_name <- if (archive) {
    .restore_table_basename(file_stem) %||% file_stem
  } else {
    file_stem
  }
  list(
    file_name    = file_stem,
    table        = table_name,
    schema       = schema_name,
    last_mod     = dttm,
    last_mod_str = raw
  )
}

# Scan all .parquet files in a directory and return a list of row lists.
.scan_dir <- function(dir, schema_name, archive = FALSE) {
  if (!dir.exists(dir)) return(list())
  files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE)
  lapply(files, function(f) {
    .scan_row(f, tools::file_path_sans_ext(basename(f)), schema_name, archive = archive)
  })
}

# Recover the original table name from an archived file stem, stripping
# the _YYYYMMDDTHHMMSSz or _unknown suffix.
.restore_table_basename <- function(stem) {
  m <- regmatches(stem,
    regexpr("^(.+)_(?:\\d{8}T\\d{6}Z|unknown_modified)$", stem, perl = TRUE))
  if (length(m) == 0) return(NULL)
  sub("_(?:\\d{8}T\\d{6}Z|unknown_modified)$", "", stem, perl = TRUE)
}

# Convert a list of row lists to a sorted tibble.
.rows_to_tibble <- function(rows) {
  if (length(rows) == 0) {
    return(tibble::tibble(
      file_name    = character(),
      table        = character(),
      schema       = character(),
      last_mod     = as.POSIXct(character(), tz = "UTC"),
      last_mod_str = character()
    ))
  }
  df <- tibble::tibble(
    file_name    = vapply(rows, `[[`, character(1), "file_name"),
    table        = vapply(rows, `[[`, character(1), "table"),
    schema       = vapply(rows, `[[`, character(1), "schema"),
    last_mod     = as.POSIXct(
      vapply(rows, function(r) {
        d <- r[["last_mod"]]
        if (is.null(d)) NA_real_ else as.numeric(d)
      }, numeric(1)),
      tz = "UTC", origin = "1970-01-01"
    ),
    last_mod_str = vapply(rows, `[[`, character(1), "last_mod_str")
  )
  df[order(df$schema, df$table, df$file_name), ]
}

# NULL-coalescing operator (base R doesn't have one)
`%||%` <- function(x, y) if (is.null(x)) y else x
