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

test_that("benchmark_tbl_to_pq benchmarks selected methods serially", {
  skip_if_not_installed("dbplyr")

  local_con <- dbplyr::simulate_dbi()
  lazy_tbl <- dbplyr::lazy_frame(
    x = 1,
    con = local_con,
    .name = "test_table"
  )

  calls <- character()

  restore_lazy <- local_rebind(
    ".benchmark_lazy_tbl_to_pq",
    function(tbl, out_dir, chunk_size, metadata, col_types) {
      calls <<- c(calls, "lazy_tbl_to_pq")
      expect_identical(tbl, lazy_tbl)
      expect_identical(chunk_size, 42L)
      expect_identical(metadata, list(source = "test"))
      expect_identical(col_types, list(x = "int32"))
      data.frame(
        path = "lazy_tbl_to_pq",
        elapsed = 1,
        total_ram_mib = 2,
        peak_ram_mib = 3,
        rows = 4,
        file_size = 5,
        stringsAsFactors = FALSE
      )
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_lazy(), add = TRUE)

  restore_dev <- local_rebind(
    ".benchmark_tbl_to_pq_dev",
    function(tbl, out_dir, chunk_size, metadata, col_types) {
      calls <<- c(calls, "tbl_to_pq")
      expect_identical(tbl, lazy_tbl)
      expect_identical(chunk_size, 42L)
      data.frame(
        path = "tbl_to_pq",
        elapsed = 6,
        total_ram_mib = 7,
        peak_ram_mib = 8,
        rows = 9,
        file_size = 10,
        stringsAsFactors = FALSE
      )
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_dev(), add = TRUE)

  restore_parquetize <- local_rebind(
    ".benchmark_parquetize",
    function(con, sql, out_dir, max_rows) {
      calls <<- c(calls, "parquetize")
      expect_identical(con, local_con)
      expect_identical(sql, "SELECT *\nFROM \"test_table\"")
      expect_identical(max_rows, 250L)
      data.frame(
        path = "parquetize",
        elapsed = 11,
        total_ram_mib = 12,
        peak_ram_mib = 13,
        rows = 14,
        file_size = 15,
        stringsAsFactors = FALSE
      )
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_parquetize(), add = TRUE)

  restore_require <- local_rebind(
    ".require_benchmark_packages",
    function(methods) invisible(methods),
    env = asNamespace("db2pq")
  )
  on.exit(restore_require(), add = TRUE)

  out <- db2pq::benchmark_tbl_to_pq(
    tbl = lazy_tbl,
    methods = c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"),
    chunk_size = 42L,
    parquetize_max_rows = 250L,
    metadata = list(source = "test"),
    col_types = list(x = "int32")
  )

  expect_identical(calls, c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"))
  expect_identical(out$path, c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"))
})

test_that("benchmark_tbl_to_pq checks required packages", {
  restore_require_namespace <- local_rebind(
    "requireNamespace",
    function(package, quietly = TRUE) package != "peakRAM",
    env = baseenv()
  )
  on.exit(restore_require_namespace(), add = TRUE)

  expect_error(
    db2pq:::.require_benchmark_packages(c("lazy_tbl_to_pq")),
    "peakRAM"
  )
})
