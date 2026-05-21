WRDS_HOST <- "wrds-pgdata.wharton.upenn.edu"
WRDS_PORT <- 9737L
WRDS_DB <- "wrds"

#' Resolve a WRDS username
#'
#' Resolves a WRDS username from, in order, an explicit argument, `WRDS_ID`,
#' `WRDS_USER`, and the keyring entry written by `wrds::wrds_set_credentials()`.
#'
#' @param wrds_id Optional WRDS username override.
#' @param user_key Keyring key used by the `wrds` package for the WRDS username.
#'
#' @return A non-empty character string.
#' @export
wrds_get_username <- function(wrds_id = NULL, user_key = "wrds_user") {
  if (!is.null(wrds_id) && nzchar(wrds_id)) {
    return(as.character(wrds_id))
  }

  env_user <- Sys.getenv("WRDS_ID", unset = "")
  if (nzchar(env_user)) {
    return(env_user)
  }

  env_user <- Sys.getenv("WRDS_USER", unset = "")
  if (nzchar(env_user)) {
    return(env_user)
  }

  tryCatch(
    keyring::key_get(user_key),
    error = function(e) {
      stop(
        "Could not resolve a WRDS username. Provide `wrds_id`, set `WRDS_ID` ",
        "or `WRDS_USER`, or run `wrds::wrds_set_credentials()`.",
        call. = FALSE
      )
    }
  )
}

#' Build WRDS PostgreSQL connection information
#'
#' @param wrds_id Optional WRDS username override.
#' @param format Return format. `"uri"` returns a PostgreSQL URI. `"dbi"`
#'   returns a list suitable for `DBI::dbConnect(RPostgres::Postgres(), !!!x)`.
#' @param sslmode PostgreSQL SSL mode. WRDS requires SSL; the default is
#'   `"require"`.
#'
#' @return A character URI or a named list of DBI connection arguments.
#' @export
wrds_conninfo <- function(wrds_id = NULL, format = c("uri", "dbi"),
                          sslmode = "require") {
  format <- match.arg(format)
  user <- wrds_get_username(wrds_id)

  if (identical(format, "dbi")) {
    return(list(
      host = WRDS_HOST,
      port = WRDS_PORT,
      dbname = WRDS_DB,
      user = user,
      sslmode = sslmode
    ))
  }

  .postgres_host_uri(
    dbname = WRDS_DB,
    host = WRDS_HOST,
    port = as.character(WRDS_PORT),
    user = user,
    sslmode = sslmode
  )
}

#' Check WRDS PostgreSQL credentials
#'
#' Attempts a small WRDS PostgreSQL connection using `RPostgres`. The return
#' value is a data frame so it can be printed or inspected in setup scripts.
#'
#' @param wrds_id Optional WRDS username override.
#' @param password Optional password. If omitted, PostgreSQL may use `.pgpass`,
#'   `PGPASSWORD`, or another libpq-supported mechanism.
#'
#' @return A one-row tibble with `ok`, `method`, and `message` columns.
#' @export
wrds_check_credentials <- function(wrds_id = NULL, password = NULL) {
  info <- wrds_conninfo(wrds_id, format = "dbi")
  if (!is.null(password)) {
    info$password <- password
  }

  result <- tryCatch({
    con <- do.call(DBI::dbConnect, c(list(drv = RPostgres::Postgres()), info))
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    DBI::dbGetQuery(con, "SELECT 1 AS ok")
    list(ok = TRUE, message = "WRDS PostgreSQL connection succeeded.")
  }, error = function(e) {
    list(ok = FALSE, message = conditionMessage(e))
  })

  tibble::tibble(
    ok = result$ok,
    method = if (pgpass_has_entry(
      host = info$host,
      port = info$port,
      database = info$dbname,
      user = info$user
    )) ".pgpass/libpq" else "libpq/keyring/env",
    message = result$message
  )
}

#' Default PostgreSQL password file path
#'
#' @return The platform-specific default `.pgpass` path.
#' @export
pgpass_path <- function() {
  if (identical(.Platform$OS.type, "windows")) {
    appdata <- Sys.getenv("APPDATA", unset = file.path(path.expand("~"), "AppData", "Roaming"))
    return(file.path(appdata, "postgresql", "pgpass.conf"))
  }
  file.path(path.expand("~"), ".pgpass")
}

#' Find a matching `.pgpass` entry
#'
#' @param host,port,database,user PostgreSQL connection target.
#' @param passfile Path to the password file.
#'
#' @return A tibble with matching entries. The `password` column is included
#'   because this is a low-level credential helper; avoid printing it in logs.
#' @export
pgpass_find <- function(host, port = 5432L, database, user,
                        passfile = pgpass_path()) {
  entries <- .pgpass_read(passfile)
  if (nrow(entries) == 0L) {
    return(entries)
  }

  target <- list(
    host = as.character(host),
    port = as.character(port),
    database = as.character(database),
    user = as.character(user)
  )

  entries[
    .pgpass_field_matches(entries$host, target$host) &
      .pgpass_field_matches(entries$port, target$port) &
      .pgpass_field_matches(entries$database, target$database) &
      .pgpass_field_matches(entries$user, target$user),
    ,
    drop = FALSE
  ]
}

#' Check for a matching `.pgpass` entry
#'
#' @inheritParams pgpass_find
#'
#' @return `TRUE` if a matching entry exists, otherwise `FALSE`.
#' @export
pgpass_has_entry <- function(host, port = 5432L, database, user,
                             passfile = pgpass_path()) {
  nrow(pgpass_find(host, port, database, user, passfile)) > 0L
}

.pgpass_read <- function(passfile) {
  cols <- c("host", "port", "database", "user", "password")
  if (!file.exists(passfile)) {
    return(tibble::as_tibble(stats::setNames(
      replicate(5L, character(), simplify = FALSE),
      cols
    )))
  }

  lines <- readLines(passfile, warn = FALSE)
  lines <- lines[nzchar(lines) & !grepl("^\\s*#", lines)]
  parsed <- lapply(lines, .pgpass_parse_line)
  parsed <- parsed[lengths(parsed) == 5L]

  if (length(parsed) == 0L) {
    return(tibble::as_tibble(stats::setNames(
      replicate(5L, character(), simplify = FALSE),
      cols
    )))
  }

  out <- as.data.frame(do.call(rbind, parsed), stringsAsFactors = FALSE)
  names(out) <- cols
  tibble::as_tibble(out)
}

.pgpass_parse_line <- function(line) {
  chars <- strsplit(line, "", fixed = TRUE)[[1]]
  fields <- character()
  current <- character()
  escaped <- FALSE

  for (ch in chars) {
    if (escaped) {
      current <- c(current, ch)
      escaped <- FALSE
    } else if (identical(ch, "\\")) {
      escaped <- TRUE
    } else if (identical(ch, ":")) {
      fields <- c(fields, paste0(current, collapse = ""))
      current <- character()
    } else {
      current <- c(current, ch)
    }
  }

  c(fields, paste0(current, collapse = ""))
}

.pgpass_field_matches <- function(field, value) {
  field == "*" | field == value
}

.wrds_connect_dbi <- function(wrds_id = NULL) {
  if (is.null(wrds_id)) {
    return(wrds::wrds_connect())
  }

  info <- wrds_conninfo(wrds_id, format = "dbi")
  do.call(DBI::dbConnect, c(list(drv = RPostgres::Postgres()), info))
}
