#' Resolve the Parquet data repository directory
#'
#' Resolves the root directory used for Parquet data from an explicit argument
#' or the `DATA_DIR` environment variable. In an interactive session, when
#' neither is available, the helper asks the user to choose or enter a
#' directory and offers to persist it in either project-level `.Renviron` or
#' user-level `~/.Renviron`.
#'
#' @param data_dir Optional Parquet data repository root.
#' @param prompt If `TRUE`, prompt interactively when `data_dir` and `DATA_DIR`
#'   are both missing.
#' @param fallback Directory returned when prompting is disabled and no explicit
#'   or environment value is available.
#'
#' @return A directory path as a character string.
#' @export
db2pq_data_dir <- function(data_dir = NULL, prompt = interactive(),
                           fallback = ".") {
  if (!is.null(data_dir) && nzchar(data_dir)) {
    return(path.expand(data_dir))
  }

  env_data_dir <- Sys.getenv("DATA_DIR", unset = "")
  if (nzchar(env_data_dir)) {
    return(path.expand(env_data_dir))
  }

  if (!isTRUE(prompt)) {
    return(path.expand(fallback))
  }

  .prompt_data_dir(fallback = fallback)
}

.prompt_data_dir <- function(fallback = ".") {
  message("DATA_DIR has not been set.")
  use_chooser <- .ask_yes_no(
    "Use a directory chooser to select DATA_DIR? [Y/n] ",
    default = TRUE
  )
  data_dir <- if (use_chooser) {
    .choose_data_dir(default = path.expand(fallback))
  } else {
    NULL
  }
  if (is.null(data_dir) || !nzchar(data_dir)) {
    data_dir <- .prompt_data_dir_path()
  }
  if (!nzchar(data_dir)) {
    stop("DATA_DIR cannot be empty.", call. = FALSE)
  }

  data_dir <- path.expand(data_dir)
  if (!dir.exists(data_dir)) {
    create <- .ask_yes_no(
      paste0("Create DATA_DIR directory '", data_dir, "'? [Y/n] "),
      default = TRUE
    )
    if (!create) {
      stop("DATA_DIR directory does not exist: ", data_dir, call. = FALSE)
    }
    dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)
    if (!dir.exists(data_dir)) {
      stop("Could not create DATA_DIR directory: ", data_dir, call. = FALSE)
    }
  }

  Sys.setenv(DATA_DIR = data_dir)
  scope <- .prompt_data_dir_scope()
  if (!identical(scope, "none")) {
    .set_renviron_value(
      "DATA_DIR",
      data_dir,
      renviron = .renviron_path(scope)
    )
  }

  data_dir
}

.prompt_data_dir_path <- function() {
  trimws(readline("DATA_DIR path (existing directory or new path): "))
}

.choose_data_dir <- function(default = getwd()) {
  if (requireNamespace("rstudioapi", quietly = TRUE) &&
      isTRUE(tryCatch(rstudioapi::isAvailable(), error = function(e) FALSE))) {
    selected <- tryCatch(
      rstudioapi::selectDirectory(
        caption = "Select Parquet data directory",
        path = default
      ),
      error = function(e) NULL
    )
    if (!is.null(selected) && nzchar(selected)) {
      return(selected)
    }
  }

  if (identical(.Platform$OS.type, "windows")) {
    selected <- tryCatch(
      utils::choose.dir(
        default = default,
        caption = "Select Parquet data directory"
      ),
      error = function(e) NULL
    )
    if (!is.null(selected) && !is.na(selected) && nzchar(selected)) {
      return(selected)
    }
  }

  selected <- tryCatch(
    tcltk::tk_choose.dir(
      default = default,
      caption = "Select Parquet data directory"
    ),
    error = function(e) NULL
  )
  if (!is.null(selected) && !is.na(selected) && nzchar(selected)) {
    return(selected)
  }

  NULL
}

.prompt_data_dir_scope <- function() {
  repeat {
    answer <- tolower(trimws(readline(
      paste0(
        "Store DATA_DIR in project .Renviron, user ~/.Renviron, or not at all? ",
        "[project/user/no] "
      )
    )))
    if (answer %in% c("p", "project")) {
      return("project")
    }
    if (answer %in% c("u", "user")) {
      return("user")
    }
    if (answer %in% c("", "n", "no", "none")) {
      return("none")
    }
    message("Enter 'project', 'user', or 'no'.")
  }
}

.ask_yes_no <- function(prompt, default = FALSE) {
  answer <- tolower(trimws(readline(prompt)))
  if (!nzchar(answer)) {
    return(isTRUE(default))
  }
  answer %in% c("y", "yes")
}

.renviron_path <- function(scope = c("user", "project")) {
  scope <- match.arg(scope)
  if (identical(scope, "project")) {
    return(file.path(getwd(), ".Renviron"))
  }
  file.path(path.expand("~"), ".Renviron")
}

.set_renviron_value <- function(name, value, renviron) {
  if (!nzchar(name) || !nzchar(value)) {
    stop("`.Renviron` variable name and value must be non-empty.", call. = FALSE)
  }

  lines <- if (file.exists(renviron)) {
    readLines(renviron, warn = FALSE)
  } else {
    character()
  }

  pattern <- paste0("^\\s*", name, "\\s*=")
  value_lines <- grepl(pattern, lines)
  env_line <- paste0(name, "=", value)
  if (any(value_lines)) {
    lines[which(value_lines)[[1]]] <- env_line
    duplicate_lines <- which(value_lines)[-1L]
    if (length(duplicate_lines) > 0L) {
      lines <- lines[-duplicate_lines]
    }
  } else {
    lines <- c(lines, env_line)
  }

  dir.create(dirname(renviron), recursive = TRUE, showWarnings = FALSE)
  writeLines(lines, renviron)
  message("Stored ", name, " in ", renviron, ".")
  invisible(renviron)
}
