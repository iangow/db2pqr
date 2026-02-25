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
#' @seealso \code{\link{db_to_pq}}, \code{\link{wrds_schema_to_pq}}
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
    chunk_size = 100000L) {

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

  wrds_date <- .parse_wrds_date(wrds_comment)

  if (!force) {
    pq_date <- .get_pq_date(out_file)
    tbl_label <- paste0(schema, ".", out_name)

    if (!is.null(wrds_date) && !is.null(pq_date) && wrds_date <= pq_date) {
      message(tbl_label, " already up to date.")
      return(invisible(NULL))
    }

    if (!is.null(wrds_date)) {
      message("Updated ", tbl_label, " is available.")
    }
  } else {
    message("Forcing update based on user request.")
  }

  message("Beginning file download at ", format(Sys.time(), tz = "UTC", usetz = TRUE), ".")

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
    metadata    = pq_metadata
  )

  message("Completed file download at ", format(Sys.time(), tz = "UTC", usetz = TRUE), ".")

  invisible(out_file)
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
    arrow::read_parquet(path, as_data_frame = FALSE)$schema$metadata,
    error = function(e) NULL
  )

  val <- meta[["last_modified"]]
  if (is.null(val) || is.na(val)) return(NULL)

  .parse_wrds_date(val)
}
