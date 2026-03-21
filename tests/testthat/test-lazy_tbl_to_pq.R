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

test_that("lazy_tbl_to_pq renders SQL and forwards to sql_to_pq", {
  skip_if_not_installed("dbplyr")

  local_con <- dbplyr::simulate_dbi()
  lazy_tbl <- dbplyr::lazy_frame(
    x = 1,
    y = 2,
    con = local_con,
    .name = "test_table"
  )

  restore_sql_to_pq <- local_rebind(
    "sql_to_pq",
    function(con, sql, out_file, chunk_size, metadata, col_types) {
      expect_identical(con, local_con)
      expect_identical(sql, "SELECT *\nFROM `test_table`")
      expect_identical(out_file, "out.parquet")
      expect_identical(chunk_size, 42L)
      expect_identical(metadata, list(source = "test"))
      expect_identical(col_types, list(id = "int32"))

      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_sql_to_pq(), add = TRUE)

  expect_identical(
    db2pq::lazy_tbl_to_pq(
      tbl = lazy_tbl,
      out_file = "out.parquet",
      chunk_size = 42L,
      metadata = list(source = "test"),
      col_types = list(id = "int32")
    ),
    "out.parquet"
  )
})
