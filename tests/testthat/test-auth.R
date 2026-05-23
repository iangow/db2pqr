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

test_that("wrds_get_username honors explicit and environment values", {
  old_id <- Sys.getenv("WRDS_ID", unset = NA_character_)
  old_user <- Sys.getenv("WRDS_USER", unset = NA_character_)
  on.exit({
    if (is.na(old_id)) Sys.unsetenv("WRDS_ID") else Sys.setenv(WRDS_ID = old_id)
    if (is.na(old_user)) Sys.unsetenv("WRDS_USER") else Sys.setenv(WRDS_USER = old_user)
  }, add = TRUE)

  Sys.unsetenv("WRDS_ID")
  Sys.unsetenv("WRDS_USER")
  expect_identical(db2pq::wrds_get_username("explicit"), "explicit")

  Sys.setenv(WRDS_ID = "from_id", WRDS_USER = "from_user")
  expect_identical(db2pq::wrds_get_username(), "from_id")

  Sys.unsetenv("WRDS_ID")
  expect_identical(db2pq::wrds_get_username(), "from_user")
})

test_that("wrds_conninfo builds uri and dbi args", {
  uri <- db2pq::wrds_conninfo("ian", format = "uri")
  expect_identical(
    uri,
    "postgresql://wrds-pgdata.wharton.upenn.edu:9737/wrds?user=ian&sslmode=require"
  )

  args <- db2pq::wrds_conninfo("ian", format = "dbi")
  expect_identical(args$host, "wrds-pgdata.wharton.upenn.edu")
  expect_identical(args$port, 9737L)
  expect_identical(args$dbname, "wrds")
  expect_identical(args$user, "ian")
  expect_identical(args$sslmode, "require")
})

test_that("DBI WRDS connections prefer explicit and environment usernames", {
  old_id <- Sys.getenv("WRDS_ID", unset = NA_character_)
  old_user <- Sys.getenv("WRDS_USER", unset = NA_character_)
  on.exit({
    if (is.na(old_id)) Sys.unsetenv("WRDS_ID") else Sys.setenv(WRDS_ID = old_id)
    if (is.na(old_user)) Sys.unsetenv("WRDS_USER") else Sys.setenv(WRDS_USER = old_user)
  }, add = TRUE)

  local_con <- structure(list(), class = "TestConnection")
  users <- character()

  restore_wrds_connect <- local_rebind(
    "wrds_connect",
    function(...) stop("Unexpected wrds fallback"),
    env = asNamespace("wrds")
  )
  on.exit(restore_wrds_connect(), add = TRUE)

  restore_db_connect <- local_rebind(
    "dbConnect",
    function(drv, ...) {
      info <- list(...)
      users <<- c(users, info$user)
      local_con
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_db_connect(), add = TRUE)

  Sys.setenv(WRDS_ID = "from_id", WRDS_USER = "from_user")
  expect_identical(db2pq:::.wrds_connect_dbi(), local_con)
  expect_identical(db2pq:::.wrds_connect_dbi("explicit"), local_con)

  Sys.unsetenv("WRDS_ID")
  expect_identical(db2pq:::.wrds_connect_dbi(), local_con)
  expect_identical(users, c("from_id", "explicit", "from_user"))
})

test_that("DBI WRDS connections fall back to wrds keyring connection", {
  old_id <- Sys.getenv("WRDS_ID", unset = NA_character_)
  old_user <- Sys.getenv("WRDS_USER", unset = NA_character_)
  on.exit({
    if (is.na(old_id)) Sys.unsetenv("WRDS_ID") else Sys.setenv(WRDS_ID = old_id)
    if (is.na(old_user)) Sys.unsetenv("WRDS_USER") else Sys.setenv(WRDS_USER = old_user)
  }, add = TRUE)
  Sys.unsetenv("WRDS_ID")
  Sys.unsetenv("WRDS_USER")

  fallback_con <- structure(list(), class = "TestConnection")

  restore_key_get <- local_rebind(
    "key_get",
    function(...) "keyring-user",
    env = asNamespace("keyring")
  )
  on.exit(restore_key_get(), add = TRUE)

  restore_wrds_connect <- local_rebind(
    "wrds_connect",
    function(...) fallback_con,
    env = asNamespace("wrds")
  )
  on.exit(restore_wrds_connect(), add = TRUE)

  restore_db_connect <- local_rebind(
    "dbConnect",
    function(...) stop("Unexpected direct DBI connection"),
    env = asNamespace("DBI")
  )
  on.exit(restore_db_connect(), add = TRUE)

  expect_identical(db2pq:::.wrds_connect_dbi(), fallback_con)
})

test_that("wrds_get_username prompts after configured sources fail", {
  old_id <- Sys.getenv("WRDS_ID", unset = NA_character_)
  old_user <- Sys.getenv("WRDS_USER", unset = NA_character_)
  on.exit({
    if (is.na(old_id)) Sys.unsetenv("WRDS_ID") else Sys.setenv(WRDS_ID = old_id)
    if (is.na(old_user)) Sys.unsetenv("WRDS_USER") else Sys.setenv(WRDS_USER = old_user)
  }, add = TRUE)
  Sys.unsetenv("WRDS_ID")
  Sys.unsetenv("WRDS_USER")

  restore_key_get <- local_rebind(
    "key_get",
    function(...) stop("No keyring username"),
    env = asNamespace("keyring")
  )
  on.exit(restore_key_get(), add = TRUE)

  restore_prompt <- local_rebind(
    ".wrds_prompt_username",
    function() "prompted-user",
    env = asNamespace("db2pq")
  )
  on.exit(restore_prompt(), add = TRUE)

  expect_identical(
    db2pq::wrds_get_username(prompt = TRUE),
    "prompted-user"
  )
})

test_that(".Renviron WRDS_ID persistence replaces duplicates", {
  renviron <- tempfile()
  writeLines(c(
    "DATA_DIR=~/pq_data",
    "WRDS_ID=old-user",
    "WRDS_USER=compat-user",
    "WRDS_ID=duplicate-user"
  ), renviron)

  expect_message(
    db2pq:::.wrds_set_renviron_id("new-user", renviron = renviron),
    "Stored WRDS_ID"
  )
  expect_identical(
    readLines(renviron, warn = FALSE),
    c(
      "DATA_DIR=~/pq_data",
      "WRDS_ID=new-user",
      "WRDS_USER=compat-user"
    )
  )
})

test_that("pgpass_path honors PGPASSFILE", {
  old_pgpassfile <- Sys.getenv("PGPASSFILE", unset = NA_character_)
  on.exit({
    if (is.na(old_pgpassfile)) {
      Sys.unsetenv("PGPASSFILE")
    } else {
      Sys.setenv(PGPASSFILE = old_pgpassfile)
    }
  }, add = TRUE)

  Sys.setenv(PGPASSFILE = "~/db2pq-test-pgpass")
  expect_identical(
    db2pq::pgpass_path(),
    path.expand("~/db2pq-test-pgpass")
  )
})

test_that("pgpass entries escape and replace exact targets", {
  passfile <- tempfile()
  writeLines(c(
    "# existing entries",
    "wrds-pgdata.wharton.upenn.edu:9737:wrds:ian:old-secret",
    "wrds-pgdata.wharton.upenn.edu:9737:wrds:ian:duplicate-secret",
    "localhost:5432:demo:ian:demo-secret"
  ), passfile)

  expect_message(
    db2pq:::.pgpass_set_entry(
      host = "wrds-pgdata.wharton.upenn.edu",
      port = 9737,
      database = "wrds",
      user = "ian",
      password = "new:secret\\value",
      passfile = passfile
    ),
    "Saved WRDS PostgreSQL credentials"
  )

  wrds <- db2pq::pgpass_find(
    "wrds-pgdata.wharton.upenn.edu",
    9737,
    "wrds",
    "ian",
    passfile = passfile
  )
  expect_identical(wrds$password, "new:secret\\value")
  expect_length(grep("^wrds-pgdata", readLines(passfile, warn = FALSE)), 1L)
})

test_that("direct WRDS connections save prompted passwords after success", {
  passfile <- tempfile()
  unlink(passfile)
  local_con <- structure(list(), class = "TestConnection")

  restore_prompt <- local_rebind(
    ".wrds_prompt_password",
    function(wrds_id) {
      expect_identical(wrds_id, "ian")
      "prompted-password"
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_prompt(), add = TRUE)

  restore_db_connect <- local_rebind(
    "dbConnect",
    function(drv, ...) {
      info <- list(...)
      expect_identical(info$user, "ian")
      expect_identical(info$password, "prompted-password")
      local_con
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_db_connect(), add = TRUE)

  expect_message(
    con <- db2pq:::.wrds_connect_dbi_user(
      "ian",
      prompt = TRUE,
      passfile = passfile
    ),
    "Saved WRDS PostgreSQL credentials"
  )
  expect_identical(con, local_con)
  expect_true(db2pq::pgpass_has_entry(
    host = "wrds-pgdata.wharton.upenn.edu",
    port = 9737,
    database = "wrds",
    user = "ian",
    passfile = passfile
  ))
})

test_that("direct WRDS connections consume WRDS_PASSWORD and save pgpass", {
  old_password <- Sys.getenv("WRDS_PASSWORD", unset = NA_character_)
  on.exit({
    if (is.na(old_password)) {
      Sys.unsetenv("WRDS_PASSWORD")
    } else {
      Sys.setenv(WRDS_PASSWORD = old_password)
    }
  }, add = TRUE)
  Sys.setenv(WRDS_PASSWORD = "env-password")

  passfile <- tempfile()
  unlink(passfile)
  local_con <- structure(list(), class = "TestConnection")

  restore_prompt <- local_rebind(
    ".wrds_prompt_password",
    function(...) stop("Unexpected password prompt"),
    env = asNamespace("db2pq")
  )
  on.exit(restore_prompt(), add = TRUE)

  restore_db_connect <- local_rebind(
    "dbConnect",
    function(drv, ...) {
      info <- list(...)
      expect_identical(info$user, "ian")
      expect_identical(info$password, "env-password")
      local_con
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_db_connect(), add = TRUE)

  expect_message(
    con <- db2pq:::.wrds_connect_dbi_user(
      "ian",
      prompt = TRUE,
      passfile = passfile
    ),
    "Saved WRDS PostgreSQL credentials"
  )
  expect_identical(con, local_con)
  expect_identical(
    db2pq::pgpass_find(
      host = "wrds-pgdata.wharton.upenn.edu",
      port = 9737,
      database = "wrds",
      user = "ian",
      passfile = passfile
    )$password,
    "env-password"
  )
})

test_that("wrds_check_credentials reports saved prompted passwords", {
  passfile <- tempfile()
  unlink(passfile)
  local_con <- structure(list(), class = "TestConnection")

  restore_prompt <- local_rebind(
    ".wrds_prompt_password",
    function(wrds_id) "prompted-password",
    env = asNamespace("db2pq")
  )
  on.exit(restore_prompt(), add = TRUE)

  restore_db_connect <- local_rebind(
    "dbConnect",
    function(drv, ...) local_con,
    env = asNamespace("DBI")
  )
  on.exit(restore_db_connect(), add = TRUE)

  restore_disconnect <- local_rebind(
    "dbDisconnect",
    function(conn, ...) invisible(TRUE),
    env = asNamespace("DBI")
  )
  on.exit(restore_disconnect(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, ...) data.frame(ok = 1L),
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  expect_message(
    result <- db2pq::wrds_check_credentials(
      wrds_id = "ian",
      prompt = TRUE,
      passfile = passfile
    ),
    "Saved WRDS PostgreSQL credentials"
  )
  expect_true(result$ok)
  expect_identical(result$method, ".pgpass/libpq")
})

test_that("pgpass_find matches wildcard and escaped entries", {
  passfile <- tempfile()
  writeLines(c(
    "localhost:5432:demo:ian:secret",
    "wrds-pgdata.wharton.upenn.edu:9737:wrds:*:wrds-secret",
    "host\\:name:1111:db:user:colon-secret"
  ), passfile)

  local <- db2pq::pgpass_find("localhost", 5432, "demo", "ian", passfile = passfile)
  expect_identical(local$password, "secret")

  wrds <- db2pq::pgpass_find(
    "wrds-pgdata.wharton.upenn.edu",
    9737,
    "wrds",
    "anyone",
    passfile = passfile
  )
  expect_identical(wrds$password, "wrds-secret")

  expect_true(db2pq::pgpass_has_entry("host:name", 1111, "db", "user", passfile = passfile))
})
