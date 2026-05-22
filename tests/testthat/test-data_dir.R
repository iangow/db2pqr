local_rebind <- function(name, value, env) {
  old <- get(name, envir = env, inherits = FALSE)
  unlockBinding(name, env)
  assign(name, value, envir = env)
  lockBinding(name, env)

  function() {
    unlockBinding(name, env)
    assign(name, old, envir = env)
    lockBinding(name, env)
  }
}

test_that("db2pq_data_dir resolves explicit, environment, and fallback paths", {
  old_data_dir <- Sys.getenv("DATA_DIR", unset = NA_character_)
  on.exit({
    if (is.na(old_data_dir)) {
      Sys.unsetenv("DATA_DIR")
    } else {
      Sys.setenv(DATA_DIR = old_data_dir)
    }
  }, add = TRUE)

  Sys.unsetenv("DATA_DIR")
  expect_identical(db2pq::db2pq_data_dir("~", prompt = FALSE), path.expand("~"))
  expect_identical(db2pq::db2pq_data_dir(prompt = FALSE), ".")

  Sys.setenv(DATA_DIR = "~/pq-data")
  expect_identical(db2pq::db2pq_data_dir(prompt = FALSE), path.expand("~/pq-data"))
})

test_that("prompted DATA_DIR can create and persist a selected directory", {
  old_data_dir <- Sys.getenv("DATA_DIR", unset = NA_character_)
  on.exit({
    if (is.na(old_data_dir)) {
      Sys.unsetenv("DATA_DIR")
    } else {
      Sys.setenv(DATA_DIR = old_data_dir)
    }
  }, add = TRUE)
  Sys.unsetenv("DATA_DIR")

  root <- tempfile("db2pq-data-dir-")
  selected <- file.path(root, "pq_data")
  renviron <- tempfile()
  unlink(renviron)

  restore_choose <- local_rebind(
    ".choose_data_dir",
    function(default) selected,
    env = asNamespace("db2pq")
  )
  on.exit(restore_choose(), add = TRUE)

  restore_create <- local_rebind(
    ".ask_yes_no",
    function(prompt, default = FALSE) {
      expect_true(default)
      TRUE
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_create(), add = TRUE)

  restore_scope <- local_rebind(
    ".prompt_data_dir_scope",
    function() "project",
    env = asNamespace("db2pq")
  )
  on.exit(restore_scope(), add = TRUE)

  restore_renviron_path <- local_rebind(
    ".renviron_path",
    function(scope) {
      expect_identical(scope, "project")
      renviron
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_renviron_path(), add = TRUE)

  expect_message(
    data_dir <- db2pq::db2pq_data_dir(prompt = TRUE),
    "Stored DATA_DIR"
  )
  expect_identical(data_dir, selected)
  expect_true(dir.exists(selected))
  expect_identical(Sys.getenv("DATA_DIR"), selected)
  expect_identical(readLines(renviron, warn = FALSE), paste0("DATA_DIR=", selected))
})

test_that("prompted DATA_DIR can skip the chooser for a new path", {
  old_data_dir <- Sys.getenv("DATA_DIR", unset = NA_character_)
  on.exit({
    if (is.na(old_data_dir)) {
      Sys.unsetenv("DATA_DIR")
    } else {
      Sys.setenv(DATA_DIR = old_data_dir)
    }
  }, add = TRUE)
  Sys.unsetenv("DATA_DIR")

  selected <- file.path(tempfile("db2pq-data-dir-"), "pq_data")
  prompts <- character()

  restore_chooser <- local_rebind(
    ".choose_data_dir",
    function(...) stop("Unexpected directory chooser"),
    env = asNamespace("db2pq")
  )
  on.exit(restore_chooser(), add = TRUE)

  restore_yes_no <- local_rebind(
    ".ask_yes_no",
    function(prompt, default = FALSE) {
      prompts <<- c(prompts, prompt)
      if (grepl("chooser", prompt)) {
        return(FALSE)
      }
      expect_match(prompt, "Create DATA_DIR directory")
      TRUE
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_yes_no(), add = TRUE)

  restore_path <- local_rebind(
    ".prompt_data_dir_path",
    function() selected,
    env = asNamespace("db2pq")
  )
  on.exit(restore_path(), add = TRUE)

  restore_scope <- local_rebind(
    ".prompt_data_dir_scope",
    function() "none",
    env = asNamespace("db2pq")
  )
  on.exit(restore_scope(), add = TRUE)

  expect_message(
    data_dir <- db2pq::db2pq_data_dir(prompt = TRUE),
    "DATA_DIR has not been set"
  )
  expect_identical(data_dir, selected)
  expect_true(dir.exists(selected))
  expect_match(prompts[[1]], "Use a directory chooser")
})

test_that(".Renviron updates replace duplicate DATA_DIR entries", {
  renviron <- tempfile()
  writeLines(c(
    "DATA_DIR=old",
    "WRDS_ID=ian",
    "DATA_DIR=duplicate"
  ), renviron)

  expect_message(
    db2pq:::.set_renviron_value("DATA_DIR", "new", renviron),
    "Stored DATA_DIR"
  )
  expect_identical(
    readLines(renviron, warn = FALSE),
    c("DATA_DIR=new", "WRDS_ID=ian")
  )
})
