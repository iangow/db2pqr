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

  # Format 1: "Last modified: MM/DD/YYYY HH:MM:SS"
  m <- regmatches(comment, regexpr("(\\d{2})/(\\d{2})/(\\d{4})", comment, perl = TRUE))
  if (length(m) == 1) {
    return(as.Date(m, format = "%m/%d/%Y"))
  }

  # Format 2: "(Updated YYYY-MM-DD)"
  m <- regmatches(comment, regexpr("\\d{4}-\\d{2}-\\d{2}", comment, perl = TRUE))
  if (length(m) == 1) {
    return(as.Date(m))
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

  # Parse whichever format is present (date-only or datetime)
  d <- tryCatch(as.Date(val), error = function(e) NULL)
  d
}
