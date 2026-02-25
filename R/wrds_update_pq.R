#' Export a WRDS table to Parquet, skipping if already up to date
#'
#' Exports a table from the WRDS PostgreSQL database to a Parquet file. Before
#' downloading, the WRDS table comment is compared against the \code{last_modified}
#' metadata embedded in any existing local Parquet file. The download is skipped
#' if the local file is already up to date, making this function safe to call
#' repeatedly as part of a scheduled data refresh.
#'
#' @param table_name Name of the table in the WRDS PostgreSQL database.
#' @param schema Name of the database schema (e.g. \code{"crsp"}, \code{"comp"}).
#' @param data_dir Root directory of the local Parquet data repository. Defaults
#'   to the \code{DATA_DIR} environment variable, or \code{"."} if unset. The
#'   output file is written to \code{<data_dir>/<schema>/<table_name>.parquet}.
#' @param out_file Optional. Full path for the output Parquet file. Overrides
#'   the path derived from \code{data_dir}, \code{schema}, and \code{table_name}.
#' @param force If \code{TRUE}, download proceeds regardless of the date
#'   comparison result.
#' @param where Optional SQL \code{WHERE} clause (without the \code{WHERE}
#'   keyword) to filter rows before export. For example,
#'   \code{where = "date > '2020-01-01'"}.
#' @param obs Optional integer. Limits the number of rows imported using SQL
#'   \code{LIMIT}. Useful for testing with large tables
#'   (e.g. \code{obs = 1000}).
#' @param keep Optional character vector of regex patterns. Only columns whose
#'   names match at least one pattern are retained. Applied after \code{drop}.
#' @param drop Optional character vector of regex patterns. Columns whose names
#'   match at least one pattern are removed. Applied before \code{keep}.
#' @param col_types Optional named list specifying column type overrides. Values
#'   may be string type names (e.g. \code{"int32"}, \code{"float32"},
#'   \code{"date"}) or Arrow \code{DataType} objects. Only columns that need to
#'   differ from their inferred types need to be supplied. See
#'   \code{\link{arrow_type}} for supported names. For example,
#'   \code{col_types = list(permno = "int32", ret = "float32")}.
#' @param tz Time zone used to interpret \code{TIMESTAMP WITHOUT TIME ZONE}
#'   columns. Such columns are cast to \code{TIMESTAMPTZ} in the SQL query using
#'   \code{AT TIME ZONE}, so they are written as UTC-normalised timestamps in the
#'   Parquet file. Defaults to \code{"UTC"}. Set to \code{NULL} to leave naive
#'   timestamps as-is.
#' @param archive If \code{TRUE}, the existing local Parquet file (if any) is
#'   moved to the archive subdirectory before being replaced. The archived
#'   filename is \code{<table>_<YYYYMMDDTHHMMSSz>.parquet}, where the timestamp
#'   suffix is derived from the \code{last_modified} metadata embedded in the
#'   existing file (i.e. the date it was last downloaded, not the incoming
#'   WRDS table comment).
#' @param archive_dir Name of the archive subdirectory relative to the schema
#'   directory. Defaults to \code{"archive"}.
#' @param alt_table_name Optional. Alternative basename for the output Parquet
#'   file, used when the file should have a different name from \code{table_name}.
#' @param chunk_size Number of rows fetched and written per chunk. Default is
#'   \code{100000}.
#'
#' @return Invisibly returns the path to the Parquet file if written, or
#'   \code{NULL} if the update was skipped.
#'
#' @examples
#' \dontrun{
#' wrds_update_pq("dsi", "crsp")
#' wrds_update_pq("feed21_bankruptcy_notification", "audit")
#'
#' # Force re-download even if local file is current
#' wrds_update_pq("dsi", "crsp", force = TRUE)
#'
#' # Limit columns and rows (useful for testing)
#' wrds_update_pq("dsf", "crsp", obs = 1000, keep = c("permno", "date", "ret"))
#' }
#'
#' @seealso \code{\link{pq_last_modified}}, \code{\link{pq_archive}}, \code{\link{pq_restore}}
#' @export
wrds_update_pq <- function(
    table_name,
    schema,
    data_dir = Sys.getenv("DATA_DIR", "."),
    out_file = NULL,
    force = FALSE,
    where = NULL,
    obs = NULL,
    keep = NULL,
    drop = NULL,
    alt_table_name = NULL,
    chunk_size = 100000L,
    col_types = NULL,
    tz = "UTC",
    archive = FALSE,
    archive_dir = "archive") {

  out_name <- if (!is.null(alt_table_name)) alt_table_name else table_name
  if (is.null(out_file)) {
    out_file <- file.path(data_dir, schema, paste0(out_name, ".parquet"))
  }

  con <- wrds::wrds_connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Fetch the WRDS table comment, which encodes the last-modified date
  wrds_comment <- DBI::dbGetQuery(
    con,
    "SELECT obj_description(to_regclass($1), 'pg_class') AS comment",
    params = list(paste0(schema, ".", table_name))
  )$comment[[1]]

  tbl_label <- paste0(schema, ".", out_name)

  if (is.null(wrds_comment) || is.na(wrds_comment)) {
    message("No comment found for ", tbl_label, ".")
  }

  wrds_date <- .parse_wrds_date(wrds_comment)

  if (!force) {
    pq_date <- .get_pq_date(out_file)

    # Conservative: if we can't establish a source date, don't update
    if (is.null(wrds_date)) {
      message(tbl_label, " already up to date.")
      return(invisible(NULL))
    }

    if (!is.null(pq_date) && wrds_date <= pq_date) {
      message(tbl_label, " already up to date.")
      return(invisible(NULL))
    }

    message("Updated ", tbl_label, " is available.")
  } else {
    message("Forcing update based on user request.")
  }

  message("Beginning file download at ", format(Sys.time(), tz = "UTC", usetz = TRUE), ".")

  if (archive && file.exists(out_file)) {
    existing_comment <- tryCatch(
      arrow::open_dataset(out_file)$schema$metadata[["last_modified"]],
      error = function(e) NULL
    )
    .archive_pq(out_file, out_name, existing_comment, archive_dir)
  }

  pq_metadata <- if (!is.null(wrds_comment)) list(last_modified = wrds_comment) else NULL

  out_file <- db_to_pq(
    table_name  = table_name,
    schema      = schema,
    data_dir    = data_dir,
    out_file    = out_file,
    where       = where,
    obs         = obs,
    keep        = keep,
    drop        = drop,
    alt_table_name = alt_table_name,
    chunk_size  = chunk_size,
    con         = con,
    metadata    = pq_metadata,
    col_types   = col_types,
    tz          = tz
  )

  message("Completed file download at ", format(Sys.time(), tz = "UTC", usetz = TRUE), ".")

  invisible(out_file)
}

# Move an existing Parquet file into the archive subdirectory before replacement.
# The archived filename is <table>_<suffix>.parquet, where suffix is derived from
# the existing file's last_modified metadata (formatted as YYYYMMDDTHHMMSSz),
# or "unknown" if unparseable or absent.
.archive_pq <- function(out_file, table_name, wrds_comment, archive_dir) {
  archive_path <- file.path(dirname(out_file), archive_dir)
  dir.create(archive_path, recursive = TRUE, showWarnings = FALSE)

  wrds_dttm <- .parse_wrds_datetime(wrds_comment)
  suffix <- if (!is.null(wrds_dttm)) {
    format(wrds_dttm, "%Y%m%dT%H%M%SZ", tz = "UTC")
  } else {
    "unknown"
  }

  dest <- file.path(archive_path, paste0(table_name, "_", suffix, ".parquet"))
  file.rename(out_file, dest)
  message("Archived existing file to: ", dest)
  invisible(dest)
}

# Parse a UTC POSIXct from a WRDS table comment string.
# Format 1 ("Last modified: MM/DD/YYYY HH:MM:SS") is interpreted as America/New_York.
# Format 2 ("(Updated YYYY-MM-DD)") is interpreted as 02:00 America/New_York,
# matching the Python package convention.
# Returns a POSIXct in UTC, or NULL if no datetime can be parsed.
.parse_wrds_datetime <- function(comment) {
  if (is.null(comment) || is.na(comment)) return(NULL)

  ny <- "America/New_York"

  # Format 1: "Last modified: MM/DD/YYYY HH:MM:SS" (Eastern time)
  if (startsWith(trimws(comment), "Last modified:")) {
    m <- regmatches(comment,
      regexpr("\\d{2}/\\d{2}/\\d{4}\\s+\\d{2}:\\d{2}:\\d{2}", comment, perl = TRUE))
    if (length(m) == 1) {
      dttm <- as.POSIXct(m, format = "%m/%d/%Y %H:%M:%S", tz = ny)
      return(as.POSIXct(format(dttm, tz = "UTC"), tz = "UTC"))
    }
  }

  # Format 2: "(Updated YYYY-MM-DD)" — treat as 02:00 America/New_York
  m <- regmatches(comment,
    regexpr("\\(Updated\\s+(\\d{4}-\\d{2}-\\d{2})\\)\\s*$", comment, perl = TRUE))
  if (length(m) == 1) {
    d <- regmatches(m, regexpr("\\d{4}-\\d{2}-\\d{2}", m, perl = TRUE))
    dttm <- as.POSIXct(paste(d, "02:00:00"), format = "%Y-%m-%d %H:%M:%S", tz = ny)
    return(as.POSIXct(format(dttm, tz = "UTC"), tz = "UTC"))
  }

  NULL
}

# Parse a date from a WRDS table comment string.
# Recognises two formats used by WRDS:
#   "Last modified: MM/DD/YYYY HH:MM:SS"
#   "... (Updated YYYY-MM-DD)"
# Returns a Date, or NULL if no date can be found.
.parse_wrds_date <- function(comment) {
  if (is.null(comment) || is.na(comment)) return(NULL)

  # Format 1: starts with "Last modified: MM/DD/YYYY HH:MM:SS"
  if (startsWith(trimws(comment), "Last modified:")) {
    m <- regmatches(comment, regexpr("(\\d{2})/(\\d{2})/(\\d{4})", comment, perl = TRUE))
    if (length(m) == 1) {
      return(as.Date(m, format = "%m/%d/%Y"))
    }
  }

  # Format 2: "... (Updated YYYY-MM-DD)" anchored at end of string
  m <- regmatches(comment, regexpr("\\(Updated\\s+(\\d{4}-\\d{2}-\\d{2})\\)\\s*$", comment, perl = TRUE))
  if (length(m) == 1) {
    d <- regmatches(m, regexpr("\\d{4}-\\d{2}-\\d{2}", m, perl = TRUE))
    return(as.Date(d))
  }

  NULL
}

# Read the last_modified date embedded in a Parquet file's schema metadata.
# Returns a Date, or NULL if the file doesn't exist or has no such metadata.
.get_pq_date <- function(path) {
  if (!file.exists(path)) return(NULL)

  meta <- tryCatch(
    arrow::open_dataset(path)$schema$metadata,
    error = function(e) NULL
  )

  val <- meta[["last_modified"]]
  if (is.null(val) || is.na(val)) return(NULL)

  .parse_wrds_date(val)
}
