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
    function(tbl, out_dir, chunk_size, metadata, col_types,
             workload, label, query_year, repetition) {
      calls <<- c(calls, "lazy_tbl_to_pq")
      expect_identical(tbl, lazy_tbl)
      expect_identical(chunk_size, 42L)
      expect_identical(metadata, list(source = "test"))
      expect_identical(col_types, list(x = "int32"))
      expect_identical(workload, "demo")
      expect_identical(label, "one-year")
      expect_identical(query_year, 2023L)
      expect_identical(repetition, 1L)
      data.frame(
        method = "lazy_tbl_to_pq",
        elapsed = 1,
        total_ram_mib = 2,
        peak_ram_mib = 3,
        rows = 4,
        file_size = 5,
        out_path = "lazy.parquet",
        workload = "demo",
        label = "one-year",
        query_year = 2023L,
        chunk_size = 42L,
        repetition = 1L,
        schema = "schema_a",
        schema_fingerprint = "abc",
        schema_mismatch = FALSE,
        stringsAsFactors = FALSE
      )
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_lazy(), add = TRUE)

  restore_dev <- local_rebind(
    ".benchmark_tbl_to_pq_dev",
    function(tbl, out_dir, chunk_size, metadata, col_types,
             workload, label, query_year, repetition) {
      calls <<- c(calls, "tbl_to_pq")
      expect_identical(tbl, lazy_tbl)
      expect_identical(chunk_size, 42L)
      expect_identical(workload, "demo")
      expect_identical(label, "one-year")
      expect_identical(query_year, 2023L)
      expect_identical(repetition, 1L)
      data.frame(
        method = "tbl_to_pq",
        elapsed = 6,
        total_ram_mib = 7,
        peak_ram_mib = 8,
        rows = 9,
        file_size = 10,
        out_path = "arrow.parquet",
        workload = "demo",
        label = "one-year",
        query_year = 2023L,
        chunk_size = 42L,
        repetition = 1L,
        schema = "schema_b",
        schema_fingerprint = "def",
        schema_mismatch = FALSE,
        stringsAsFactors = FALSE
      )
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_dev(), add = TRUE)

  restore_parquetize <- local_rebind(
    ".benchmark_parquetize",
    function(tbl, out_dir, max_rows, workload, label, query_year, repetition) {
      calls <<- c(calls, "parquetize")
      expect_identical(tbl, lazy_tbl)
      expect_identical(max_rows, 250L)
      expect_identical(workload, "demo")
      expect_identical(label, "one-year")
      expect_identical(query_year, 2023L)
      expect_identical(repetition, 1L)
      data.frame(
        method = "parquetize",
        elapsed = 11,
        total_ram_mib = 12,
        peak_ram_mib = 13,
        rows = 14,
        file_size = 15,
        out_path = "pq_dir",
        workload = "demo",
        label = "one-year",
        query_year = 2023L,
        chunk_size = 250L,
        repetition = 1L,
        schema = "schema_a",
        schema_fingerprint = "abc",
        schema_mismatch = FALSE,
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
    col_types = list(x = "int32"),
    workload = "demo",
    label = "one-year",
    query_year = 2023L,
    repetition = 1L
  )

  expect_identical(calls, c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"))
  expect_identical(out$method, c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"))
  expect_true(all(out$schema_mismatch))
})

test_that("benchmark_tbl_to_pq package requirements succeed when installed", {
  expect_no_error(
    db2pq:::.require_benchmark_packages(c("lazy_tbl_to_pq"))
  )
})

test_that("tbl_to_pq_parquetize renders SQL and forwards to parquetize", {
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("parquetize")

  local_con <- dbplyr::simulate_dbi()
  lazy_tbl <- dbplyr::lazy_frame(
    x = 1,
    y = 2,
    con = local_con,
    .name = "test_table"
  )

  restore_parquetize <- local_rebind(
    "dbi_to_parquet",
    function(conn, sql_query, path_to_parquet, max_rows, ...) {
      expect_identical(conn, local_con)
      expect_identical(sql_query, "SELECT *\nFROM \"test_table\"")
      expect_identical(path_to_parquet, "out_dir")
      expect_identical(max_rows, 250L)
      invisible(TRUE)
    },
    env = asNamespace("parquetize")
  )
  on.exit(restore_parquetize(), add = TRUE)

  expect_identical(
    db2pq::tbl_to_pq_parquetize(
      tbl = lazy_tbl,
      out_path = "out_dir",
      max_rows = 250L
    ),
    "out_dir"
  )
})

test_that("benchmark_workload_set repeats workloads and returns a summary", {
  skip_if_not_installed("dbplyr")

  local_con <- dbplyr::simulate_dbi()
  workloads <- list(
    crsp_dsi = dbplyr::lazy_frame(x = 1, con = local_con, .name = "dsi"),
    crsp_dsf = dbplyr::lazy_frame(x = 1, con = local_con, .name = "dsf")
  )

  calls <- list()

  restore_benchmark <- local_rebind(
    "benchmark_tbl_to_pq",
    function(tbl, methods, out_dir, chunk_size, parquetize_max_rows, metadata,
             col_types, workload, label, query_year, repetition) {
      calls[[length(calls) + 1L]] <<- list(
        tbl = tbl,
        methods = methods,
        workload = workload,
        repetition = repetition,
        year = query_year,
        label = label,
        chunk_size = chunk_size
      )

      data.frame(
        method = methods,
        elapsed = c(2, 4)[seq_along(methods)],
        total_ram_mib = c(10, 12)[seq_along(methods)],
        peak_ram_mib = c(20, 22)[seq_along(methods)],
        rows = 100L,
        file_size = 200L,
        out_path = paste0(workload, "_", repetition, ".parquet"),
        workload = workload,
        label = label,
        query_year = query_year,
        chunk_size = chunk_size,
        repetition = repetition,
        schema = "schema_a",
        schema_fingerprint = "abc",
        schema_mismatch = FALSE,
        stringsAsFactors = FALSE
      )
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_benchmark(), add = TRUE)

  out <- db2pq::benchmark_workload_set(
    workloads = workloads,
    methods = c("lazy_tbl_to_pq", "tbl_to_pq"),
    repeats = 2L,
    chunk_size = 50L,
    year = 2023L,
    label = "one-year"
  )

  expect_equal(length(calls), 4L)
  expect_equal(nrow(out$runs), 8L)
  expect_equal(nrow(out$summary), 4L)
  expect_true(all(out$summary$runs == 2L))
  expect_identical(sort(unique(out$runs$workload)), c("crsp_dsf", "crsp_dsi"))
})

test_that("build_benchmark_workloads builds one-year filters for the default tables", {
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")

  local_con <- dbplyr::simulate_dbi()

  restore_tbl <- local_rebind(
    "tbl",
    function(src, from, ...) {
      table_name <- if (inherits(from, "Id")) {
        paste(from@name, collapse = ".")
      } else {
        as.character(from)[1]
      }

      dbplyr::lazy_frame(
        date = as.Date("2023-01-01"),
        datadate = as.Date("2023-01-01"),
        con = src,
        .name = table_name
      )
    },
    env = asNamespace("dplyr")
  )
  on.exit(restore_tbl(), add = TRUE)

  workloads <- db2pq::build_benchmark_workloads(local_con, year = 2023L)

  expect_identical(names(workloads), c("crsp_dsf", "comp_funda", "crsp_dsi"))

  sql_dsf <- as.character(dbplyr::sql_render(workloads$crsp_dsf))
  sql_funda <- as.character(dbplyr::sql_render(workloads$comp_funda))

  expect_match(sql_dsf, "\"crsp\\.dsf\"")
  expect_match(sql_dsf, "2023-01-01")
  expect_match(sql_dsf, "2024-01-01")
  expect_match(sql_funda, "\"comp\\.funda\"")
  expect_match(sql_funda, "datadate")
})

test_that("schema fingerprint ignores schema-level metadata differences", {
  skip_if_not_installed("arrow")

  base_schema <- arrow::schema(
    arrow::field("x", arrow::int32()),
    arrow::field("y", arrow::string())
  )
  metadata_schema <- base_schema$WithMetadata(list(source = "parquetize"))

  base_text <- db2pq:::.schema_text_without_metadata(base_schema)
  metadata_text <- db2pq:::.schema_text_without_metadata(metadata_schema)

  expect_identical(base_text, metadata_text)
  expect_identical(
    db2pq:::.schema_fingerprint(base_text),
    db2pq:::.schema_fingerprint(metadata_text)
  )
})
