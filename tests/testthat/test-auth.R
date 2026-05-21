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
