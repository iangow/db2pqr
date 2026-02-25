#' Archive a Parquet file into the archive subdirectory
#'
#' Moves a Parquet file into an archive subdirectory, appending a timestamp
#' suffix derived from the file's \code{last_modified} metadata. The archived
#' filename takes the form \code{<table>_<YYYYMMDDTHHMMSSz>.parquet}.
#'
#' Either \code{file_name} or both \code{table_name} and \code{schema} must be
#' provided.
#'
#' @param table_name Name of the table. Required if \code{file_name} is not
#'   provided.
#' @param schema Schema name. Required if \code{file_name} is not provided.
#' @param data_dir Root directory of the Parquet data repository. Defaults to
#'   the \code{DATA_DIR} environment variable, or \code{"."} if unset.
#' @param file_name Full path to the Parquet file to archive. If provided,
#'   \code{table_name}, \code{schema}, and \code{data_dir} are ignored.
#' @param archive_dir Name of the archive subdirectory relative to the schema
#'   directory. Defaults to \code{"archive"}.
#'
#' @return Invisibly returns the path to the archived file, or \code{NULL} if
#'   the source file does not exist.
#'
#' @examples
#' \dontrun{
#' pq_archive("company", "comp")
#' pq_archive(file_name = "~/pq_data/comp/company.parquet")
#' }
#'
#' @seealso \code{\link{pq_restore}}, \code{\link{wrds_update_pq}}
#' @export
pq_archive <- function(table_name = NULL,
                       schema = NULL,
                       data_dir = Sys.getenv("DATA_DIR", "."),
                       file_name = NULL,
                       archive_dir = "archive") {
  if (!is.null(file_name)) {
    pq_file      <- normalizePath(file_name, mustWork = FALSE)
    table_basename <- tools::file_path_sans_ext(basename(pq_file))
  } else {
    if (is.null(table_name) || is.null(schema)) {
      stop("table_name and schema are required when file_name is not provided.")
    }
    pq_file       <- file.path(data_dir, schema, paste0(table_name, ".parquet"))
    table_basename <- table_name
  }

  if (!file.exists(pq_file)) {
    message("File not found: ", pq_file)
    return(invisible(NULL))
  }

  raw  <- .get_pq_raw(pq_file)
  dttm <- .parse_wrds_datetime(raw)
  suffix <- if (!is.null(dttm)) {
    format(dttm, "%Y%m%dT%H%M%SZ", tz = "UTC")
  } else {
    "unknown_modified"
  }

  archive_path <- file.path(dirname(pq_file), archive_dir)
  dir.create(archive_path, recursive = TRUE, showWarnings = FALSE)

  dest <- file.path(archive_path, paste0(table_basename, "_", suffix, ".parquet"))
  file.rename(pq_file, dest)
  message("Archived to: ", dest)
  invisible(dest)
}


#' Restore an archived Parquet file to the active schema directory
#'
#' Moves an archived Parquet file back into the schema directory, stripping
#' the timestamp suffix to recover the original filename. If a current file
#' already exists at the destination, it is archived first (when
#' \code{archive = TRUE}) or the function stops (when \code{archive = FALSE}).
#'
#' @param file_basename Basename of the archived file (with or without the
#'   \code{.parquet} extension), e.g. \code{"company_20251109T072042Z"}.
#' @param schema Schema name.
#' @param data_dir Root directory of the Parquet data repository. Defaults to
#'   the \code{DATA_DIR} environment variable, or \code{"."} if unset.
#' @param archive If \code{TRUE} (default), any existing file at the
#'   destination is archived before the restore. If \code{FALSE}, the function
#'   stops if the destination already exists.
#' @param archive_dir Name of the archive subdirectory. Defaults to
#'   \code{"archive"}.
#'
#' @return Invisibly returns the path to the restored file, or \code{NULL} on
#'   failure.
#'
#' @examples
#' \dontrun{
#' pq_restore("company_20251109T072042Z", "comp")
#' }
#'
#' @seealso \code{\link{pq_archive}}, \code{\link{pq_last_modified}}
#' @export
pq_restore <- function(file_basename,
                       schema,
                       data_dir = Sys.getenv("DATA_DIR", "."),
                       archive = TRUE,
                       archive_dir = "archive") {
  archive_dir  <- archive_dir %||% "archive"
  schema_dir   <- file.path(data_dir, schema)
  archive_path <- file.path(schema_dir, archive_dir)

  # Normalise: strip .parquet if present, then re-add
  stem         <- tools::file_path_sans_ext(basename(file_basename))
  archived_file <- file.path(archive_path, paste0(stem, ".parquet"))

  if (!file.exists(archived_file)) {
    message("Archived file not found: ", archived_file)
    return(invisible(NULL))
  }

  table_basename <- .restore_table_basename(stem)
  if (is.null(table_basename)) {
    message("Could not determine destination table name from archived file: ",
            basename(archived_file))
    return(invisible(NULL))
  }

  dest_file     <- file.path(schema_dir, paste0(table_basename, ".parquet"))
  archived_current <- NULL

  if (file.exists(dest_file)) {
    if (archive) {
      archived_current <- pq_archive(file_name = dest_file, archive_dir = archive_dir)
    } else {
      message("Destination file already exists (", dest_file,
              "); set archive = TRUE to archive it first.")
      return(invisible(NULL))
    }
  }

  ok <- tryCatch({
    file.rename(archived_file, dest_file)
    TRUE
  }, error = function(e) {
    message("Could not restore archived Parquet file: ", conditionMessage(e))
    # Best-effort rollback: if we archived the current file but rename failed,
    # move it back
    if (!is.null(archived_current) && file.exists(archived_current) &&
        !file.exists(dest_file)) {
      try(file.rename(archived_current, dest_file), silent = TRUE)
    }
    FALSE
  })

  if (!ok) return(invisible(NULL))
  message("Restored to: ", dest_file)
  invisible(dest_file)
}
