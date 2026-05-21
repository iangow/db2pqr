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

test_that("tbl_to_pq renders SQL and forwards to sql_to_pq_dev", {
  skip_if_not_installed("dbplyr")

  local_con <- dbplyr::simulate_dbi()
  lazy_tbl <- dbplyr::lazy_frame(
    x = 1,
    y = 2,
    con = local_con,
    .name = "test_table"
  )

  restore_sql_to_pq_dev <- local_rebind(
    "sql_to_pq_dev",
    function(con, sql, out_file, chunk_size, metadata, col_types) {
      expect_identical(con, local_con)
      expect_match(sql, "^SELECT \\*\\s+FROM [`\"]test_table[`\"]$")
      expect_identical(out_file, "out.parquet")
      expect_identical(chunk_size, 42L)
      expect_identical(metadata, list(source = "test"))
      expect_identical(col_types, list(id = "int32"))

      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_sql_to_pq_dev(), add = TRUE)

  expect_identical(
    db2pq::tbl_to_pq(
      tbl = lazy_tbl,
      out_file = "out.parquet",
      chunk_size = 42L,
      metadata = list(source = "test"),
      col_types = list(id = "int32")
    ),
    "out.parquet"
  )
})

test_that("adbi URI builder handles host and socket connections", {
  old <- Sys.getenv("PGPASSWORD", unset = NA_character_)
  Sys.setenv(PGPASSWORD = "s3cret")
  on.exit({
    if (is.na(old)) Sys.unsetenv("PGPASSWORD") else Sys.setenv(PGPASSWORD = old)
  }, add = TRUE)

  host_info <- list(
    host = "db.example.com",
    port = 5432L,
    dbname = "wrds",
    user = "ian"
  )

  socket_info <- list(
    host = "/tmp",
    port = 5432L,
    dbname = "iangow",
    user = "iangow"
  )

  expect_identical(
    db2pq:::.adbi_uri_from_info(host_info),
    "postgresql://db.example.com:5432/wrds?user=ian&password=s3cret"
  )

  expect_identical(
    db2pq:::.adbi_uri_from_info(socket_info),
    "postgresql:///iangow?host=%2Ftmp&port=5432&user=iangow&password=s3cret"
  )
})

test_that("con_to_adbi URI builder requires host and dbname", {
  expect_error(
    db2pq:::.adbi_uri_from_info(list(host = "", dbname = "wrds")),
    "required field `host`"
  )

  expect_error(
    db2pq:::.adbi_uri_from_info(list(host = "localhost", dbname = "")),
    "required field `dbname`"
  )
})

test_that("tbl_to_pq_debug renders SQL and forwards to sql_to_pq_dev_debug", {
  skip_if_not_installed("dbplyr")

  local_con <- dbplyr::simulate_dbi()
  lazy_tbl <- dbplyr::lazy_frame(
    x = 1,
    y = 2,
    con = local_con,
    .name = "test_table"
  )

  restore_debug <- local_rebind(
    "sql_to_pq_dev_debug",
    function(con, sql, max_chunks) {
      expect_identical(con, local_con)
      expect_match(sql, "^SELECT \\*\\s+FROM [`\"]test_table[`\"]$")
      expect_identical(max_chunks, 3)

      data.frame(
        chunk = 1L,
        rows = 10L,
        fetch_ok = TRUE,
        table_ok = TRUE,
        schema = "ok",
        error_stage = "",
        error = "",
        stringsAsFactors = FALSE
      )
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_debug(), add = TRUE)

  out <- db2pq::tbl_to_pq_debug(lazy_tbl, max_chunks = 3)
  expect_identical(out$rows[[1]], 10L)
})

test_that("nanoarrow_num_rows reads batch length", {
  batch <- nanoarrow::as_nanoarrow_array(data.frame(a = 1:3))
  expect_identical(db2pq:::.nanoarrow_num_rows(batch), 3L)
})

test_that("parse_dbplyr_table_path parses schema-qualified names", {
  out <- db2pq:::.parse_dbplyr_table_path('"crsp"."dsf"')
  expect_identical(out$schema, "crsp")
  expect_identical(out$table, "dsf")
})

test_that("pg_numeric_col_types maps numeric metadata to Arrow types", {
  local_con <- dbplyr::simulate_dbi()

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, ...) {
      expect_identical(conn, local_con)
      data.frame(
        column_name = c("prc", "ret", "vol", "weird"),
        numeric_precision = c(11, 10, 10, NA),
        numeric_scale = c(5, 6, 0, NA)
      )
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_quote <- local_rebind(
    "dbQuoteString",
    function(conn, x, ...) paste0("'", x, "'"),
    env = asNamespace("DBI")
  )
  on.exit(restore_quote(), add = TRUE)

  msg <- NULL
  out <- withCallingHandlers(
    db2pq:::.pg_numeric_col_types(local_con, "crsp", "dsf", c("prc", "ret", "vol", "weird")),
    message = function(m) {
      msg <<- conditionMessage(m)
      invokeRestart("muffleMessage")
    }
  )

  expect_identical(out$prc$ToString(), "decimal128(11, 5)")
  expect_identical(out$ret$ToString(), "decimal128(10, 6)")
  expect_identical(out$vol$ToString(), "decimal128(10, 0)")
  expect_identical(out$weird$ToString(), "double")
  expect_match(msg, "Falling back to float64")
})
