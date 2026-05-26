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
#' @param prompt If `TRUE`, prompt interactively for a WRDS username when no
#'   explicit, environment, or keyring username can be resolved.
#'
#' @return A non-empty character string.
#' @export
wrds_get_username <- function(wrds_id = NULL, user_key = "wrds_user",
                              prompt = interactive()) {
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
      if (isTRUE(prompt)) {
        return(.wrds_prompt_username())
      }
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
#' @param password Optional password. If omitted, a matching `.pgpass` entry is
#'   preferred. When `.pgpass` has no matching entry, an existing
#'   `WRDS_PASSWORD` environment variable is used before an interactive prompt.
#'   PostgreSQL may also use `PGPASSWORD` or another libpq-supported mechanism
#'   when no password is passed to the connection.
#' @param prompt If `TRUE`, prompt interactively for a WRDS PostgreSQL password
#'   when no password was supplied and no matching `.pgpass` entry exists.
#' @param save If `TRUE`, save a supplied or prompted password to `.pgpass`
#'   after a successful WRDS connection test.
#' @param passfile PostgreSQL password-file path. Defaults to `PGPASSFILE` when
#'   set, otherwise the platform default password-file path.
#'
#' @return A one-row tibble with `ok`, `method`, and `message` columns.
#' @export
wrds_check_credentials <- function(wrds_id = NULL, password = NULL,
                                   prompt = interactive(), save = TRUE,
                                   passfile = NULL) {
  passfile <- if (is.null(passfile)) pgpass_path() else path.expand(passfile)
  info <- wrds_conninfo(wrds_id, format = "dbi")
  used_pgpass <- pgpass_has_entry(
    info$host, info$port, info$dbname, info$user, passfile = passfile
  )

  result <- tryCatch({
    con <- .wrds_connect_dbi_info(
      info,
      password = password,
      prompt = prompt,
      save = save,
      passfile = passfile
    )
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    DBI::dbGetQuery(con, "SELECT 1 AS ok")
    list(ok = TRUE, message = "WRDS PostgreSQL connection succeeded.")
  }, error = function(e) {
    list(ok = FALSE, message = conditionMessage(e))
  })

  tibble::tibble(
    ok = result$ok,
    method = if (used_pgpass || pgpass_has_entry(
      info$host, info$port, info$dbname, info$user, passfile = passfile
    )) ".pgpass/libpq" else "libpq/keyring/env",
    message = result$message
  )
}

#' Connect to WRDS PostgreSQL with DBI
#'
#' Opens a `DBI` connection to the WRDS PostgreSQL database using `RPostgres`
#' and the `db2pq` WRDS authentication flow.
#'
#' The username is resolved from `wrds_id`, `WRDS_ID`, `WRDS_USER`, or the
#' keyring entry written by `wrds::wrds_set_credentials()`. Password handling
#' prefers a matching PostgreSQL `.pgpass` entry, then `WRDS_PASSWORD`, then an
#' interactive password prompt when `prompt = TRUE`. When a supplied, prompted,
#' or `WRDS_PASSWORD` password is used successfully and `save = TRUE`, it is
#' saved to `.pgpass` for future connections.
#'
#' @inheritParams wrds_check_credentials
#'
#' @return A `DBIConnection` object.
#' @export
wrds_connect_dbi <- function(wrds_id = NULL, prompt = interactive(),
                             save = TRUE, passfile = NULL) {
  passfile <- if (is.null(passfile)) pgpass_path() else path.expand(passfile)
  .wrds_connect_dbi(
    wrds_id = wrds_id,
    prompt = prompt,
    save = save,
    passfile = passfile
  )
}

#' PostgreSQL password file path
#'
#' @return The `PGPASSFILE` path when set, otherwise the platform-specific
#'   default `.pgpass` path.
#' @noRd
pgpass_path <- function() {
  pgpassfile <- Sys.getenv("PGPASSFILE", unset = "")
  if (nzchar(pgpassfile)) {
    return(path.expand(pgpassfile))
  }

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
#' @noRd
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
#' @noRd
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

.pgpass_set_entry <- function(host, port, database, user, password,
                              passfile = pgpass_path()) {
  if (!nzchar(password)) {
    stop("`password` must be a non-empty string.", call. = FALSE)
  }

  target <- c(
    host = as.character(host),
    port = as.character(port),
    database = as.character(database),
    user = as.character(user)
  )
  entry <- paste(
    vapply(c(target, password), .pgpass_escape, character(1)),
    collapse = ":"
  )

  lines <- if (file.exists(passfile)) readLines(passfile, warn = FALSE) else character()
  parsed <- lapply(lines, .pgpass_parse_line)
  exact_matches <- vapply(parsed, function(fields) {
    length(fields) == 5L && identical(unname(fields[1:4]), unname(target))
  }, logical(1))

  if (any(exact_matches)) {
    lines[which(exact_matches)[[1]]] <- entry
    duplicate_matches <- which(exact_matches)[-1L]
    if (length(duplicate_matches) > 0L) {
      lines <- lines[-duplicate_matches]
    }
  } else {
    lines <- c(lines, entry)
  }

  dir.create(dirname(passfile), recursive = TRUE, showWarnings = FALSE)
  writeLines(lines, passfile)
  if (!identical(.Platform$OS.type, "windows")) {
    Sys.chmod(passfile, mode = "0600")
  }
  message("Saved WRDS PostgreSQL credentials to ", passfile, ".")
  invisible(passfile)
}

.pgpass_escape <- function(value) {
  chars <- strsplit(as.character(value), "", fixed = TRUE)[[1]]
  escaped <- ifelse(chars %in% c("\\", ":"), paste0("\\", chars), chars)
  paste0(escaped, collapse = "")
}

.wrds_connect_dbi <- function(wrds_id = NULL, prompt = interactive(),
                              save = TRUE, passfile = pgpass_path()) {
  direct_user <- .wrds_direct_username(wrds_id)
  if (!is.null(direct_user)) {
    return(.wrds_connect_dbi_user(
      direct_user,
      prompt = prompt,
      save = save,
      passfile = passfile
    ))
  }

  wrds_get_username(prompt = prompt)
  prompted_user <- .wrds_direct_username()
  if (!is.null(prompted_user)) {
    return(.wrds_connect_dbi_user(
      prompted_user,
      prompt = prompt,
      save = save,
      passfile = passfile
    ))
  }

  wrds::wrds_connect()
}

.wrds_connect_dbi_user <- function(wrds_id, password = NULL,
                                   prompt = interactive(), save = TRUE,
                                   passfile = pgpass_path()) {
  info <- wrds_conninfo(wrds_id, format = "dbi")
  .wrds_connect_dbi_info(
    info,
    password = password,
    prompt = prompt,
    save = save,
    passfile = passfile
  )
}

.wrds_connect_dbi_info <- function(info, password = NULL,
                                   prompt = interactive(), save = TRUE,
                                   passfile = pgpass_path()) {
  has_pgpass <- pgpass_has_entry(
    info$host, info$port, info$dbname, info$user, passfile = passfile
  )
  connection_password <- password
  if (is.null(connection_password) && !has_pgpass) {
    connection_password <- .wrds_env_password()
  }
  if (is.null(connection_password) && !has_pgpass && isTRUE(prompt)) {
    connection_password <- .wrds_prompt_password(info$user)
  }
  if (!is.null(connection_password)) {
    info$password <- connection_password
  }

  con <- do.call(DBI::dbConnect, c(list(drv = RPostgres::Postgres()), info))
  if (isTRUE(save) && !has_pgpass && !is.null(connection_password)) {
    .pgpass_set_entry(
      host = info$host,
      port = info$port,
      database = info$dbname,
      user = info$user,
      password = connection_password,
      passfile = passfile
    )
  }

  con
}

.wrds_env_password <- function() {
  password <- Sys.getenv("WRDS_PASSWORD", unset = "")
  if (nzchar(password)) {
    return(password)
  }

  NULL
}

.wrds_direct_username <- function(wrds_id = NULL) {
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

  NULL
}

.wrds_prompt_username <- function() {
  if (!interactive()) {
    return(NULL)
  }

  wrds_id <- trimws(readline("WRDS username: "))
  if (!nzchar(wrds_id)) {
    stop("WRDS username cannot be empty.", call. = FALSE)
  }

  Sys.setenv(WRDS_ID = wrds_id)
  answer <- tolower(trimws(readline(
    "Store WRDS_ID in ~/.Renviron for future R sessions? [Y/n] "
  )))
  if (answer %in% c("", "y", "yes")) {
    .wrds_set_renviron_id(wrds_id)
  }

  wrds_id
}

.wrds_prompt_password <- function(wrds_id) {
  service <- paste0("db2pq_wrds_password_", Sys.getpid())
  on.exit(try(keyring::key_delete(service, username = wrds_id), silent = TRUE), add = TRUE)
  keyring::key_set(
    service,
    username = wrds_id,
    prompt = paste0("WRDS PostgreSQL password for ", wrds_id, ": ")
  )
  keyring::key_get(service, username = wrds_id)
}

.wrds_set_renviron_id <- function(wrds_id,
                                   renviron = file.path(path.expand("~"), ".Renviron")) {
  .set_renviron_value("WRDS_ID", wrds_id, renviron = renviron)
}
