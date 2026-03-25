# Development-only benchmark helpers for comparing Parquet export paths.
#
# These functions are intentionally kept out of the installed package. Load the
# package with devtools::load_all() or install it locally before sourcing this
# script.

tbl_to_pq_parquetize <- function(tbl, out_path, max_rows = 100000L) {
  if (!requireNamespace("dbplyr", quietly = TRUE)) {
    stop("Package `dbplyr` must be installed to use `tbl_to_pq_parquetize()`.")
  }
  if (!requireNamespace("parquetize", quietly = TRUE)) {
    stop("Package `parquetize` must be installed to use `tbl_to_pq_parquetize()`.")
  }

  sql <- as.character(dbplyr::sql_render(tbl))
  con <- dbplyr::remote_con(tbl)

  parquetize::dbi_to_parquet(
    conn = con,
    sql_query = sql,
    path_to_parquet = out_path,
    max_rows = max_rows
  )

  invisible(out_path)
}

benchmark_tbl_to_pq <- function(tbl,
                                methods = c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"),
                                out_dir = tempdir(),
                                chunk_size = 100000L,
                                parquetize_max_rows = chunk_size,
                                metadata = NULL,
                                col_types = NULL,
                                workload = NA_character_,
                                label = NA_character_,
                                query_year = NA_integer_,
                                repetition = NA_integer_) {
  .require_benchmark_packages(methods)

  methods <- match.arg(methods, c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"), several.ok = TRUE)

  out <- do.call(rbind, lapply(methods, function(method) {
    switch(method,
      lazy_tbl_to_pq = .benchmark_lazy_tbl_to_pq(
        tbl = tbl,
        out_dir = out_dir,
        chunk_size = chunk_size,
        metadata = metadata,
        col_types = col_types,
        workload = workload,
        label = label,
        query_year = query_year,
        repetition = repetition
      ),
      tbl_to_pq = .benchmark_tbl_to_pq_dev(
        tbl = tbl,
        out_dir = out_dir,
        chunk_size = chunk_size,
        metadata = metadata,
        col_types = col_types,
        workload = workload,
        label = label,
        query_year = query_year,
        repetition = repetition
      ),
      parquetize = .benchmark_parquetize(
        tbl = tbl,
        out_dir = out_dir,
        max_rows = parquetize_max_rows,
        workload = workload,
        label = label,
        query_year = query_year,
        repetition = repetition
      )
    )
  }))

  db2pq:::.annotate_schema_mismatch(out)
}

build_benchmark_workloads <- function(con, year = NULL, span_years = 1L) {
  if (!requireNamespace("dbplyr", quietly = TRUE) ||
      !requireNamespace("dplyr", quietly = TRUE)) {
    stop(
      "Packages `dbplyr` and `dplyr` must be installed to use `build_benchmark_workloads()`."
    )
  }

  span_years <- db2pq:::.validate_positive_count(span_years, "span_years")
  year <- db2pq:::.resolve_benchmark_year(year)
  start_date <- as.Date(sprintf("%04d-01-01", year))
  end_date <- as.Date(sprintf("%04d-01-01", year + span_years))

  list(
    crsp_dsf = dplyr::filter(
      dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsf")),
      date >= !!start_date,
      date < !!end_date
    ),
    comp_funda = dplyr::filter(
      dplyr::tbl(con, DBI::Id(schema = "comp", table = "funda")),
      datadate >= !!start_date,
      datadate < !!end_date
    ),
    crsp_dsi = dplyr::filter(
      dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsi")),
      date >= !!start_date,
      date < !!end_date
    )
  )
}

benchmark_workload_set <- function(workloads,
                                   methods = c("lazy_tbl_to_pq", "tbl_to_pq"),
                                   repeats = 3L,
                                   out_dir = tempdir(),
                                   chunk_size = 100000L,
                                   parquetize_max_rows = chunk_size,
                                   metadata = NULL,
                                   col_types = NULL,
                                   year = NA_integer_,
                                   label = NA_character_) {
  repeats <- db2pq:::.validate_positive_count(repeats, "repeats")

  if (!is.list(workloads) || length(workloads) == 0L || is.null(names(workloads))) {
    stop("`workloads` must be a named list of lazy `dbplyr` tables or queries.")
  }

  if (anyNA(names(workloads)) || any(names(workloads) == "")) {
    stop("`workloads` must have non-empty names.")
  }

  runs <- do.call(rbind, lapply(names(workloads), function(workload_name) {
    tbl <- workloads[[workload_name]]

    do.call(rbind, lapply(seq_len(repeats), function(i) {
      benchmark_tbl_to_pq(
        tbl = tbl,
        methods = methods,
        out_dir = out_dir,
        chunk_size = chunk_size,
        parquetize_max_rows = parquetize_max_rows,
        metadata = metadata,
        col_types = col_types,
        workload = workload_name,
        label = label,
        query_year = year,
        repetition = i
      )
    }))
  }))

  list(
    runs = runs,
    summary = db2pq:::.summarize_benchmark_runs(runs)
  )
}

.require_benchmark_packages <- function(methods) {
  needed <- c("dbplyr", "dplyr", "peakRAM")
  if ("parquetize" %in% methods) {
    needed <- c(needed, "parquetize")
  }

  missing <- needed[!vapply(needed, requireNamespace, quietly = TRUE, logical(1))]
  if (length(missing) > 0L) {
    stop(
      "Package(s) required for `benchmark_tbl_to_pq()` are not installed: ",
      paste(missing, collapse = ", "),
      "."
    )
  }
}

.benchmark_lazy_tbl_to_pq <- function(tbl, out_dir, chunk_size, metadata, col_types,
                                      workload, label, query_year, repetition) {
  out_file <- tempfile(pattern = "lazy_tbl_to_pq_", tmpdir = out_dir, fileext = ".parquet")
  pr <- peakRAM::peakRAM(
    db2pq::lazy_tbl_to_pq(
      tbl = tbl,
      out_file = out_file,
      chunk_size = chunk_size,
      metadata = metadata,
      col_types = col_types
    )
  )

  .benchmark_result(
    method = "lazy_tbl_to_pq",
    out_path = out_file,
    pr = pr,
    workload = workload,
    label = label,
    query_year = query_year,
    chunk_size = chunk_size,
    repetition = repetition
  )
}

.benchmark_tbl_to_pq_dev <- function(tbl, out_dir, chunk_size, metadata, col_types,
                                     workload, label, query_year, repetition) {
  out_file <- tempfile(pattern = "tbl_to_pq_", tmpdir = out_dir, fileext = ".parquet")
  pr <- peakRAM::peakRAM(
    db2pq::tbl_to_pq(
      tbl = tbl,
      out_file = out_file,
      chunk_size = chunk_size,
      metadata = metadata,
      col_types = col_types
    )
  )

  .benchmark_result(
    method = "tbl_to_pq",
    out_path = out_file,
    pr = pr,
    workload = workload,
    label = label,
    query_year = query_year,
    chunk_size = chunk_size,
    repetition = repetition
  )
}

.benchmark_parquetize <- function(tbl, out_dir, max_rows, workload, label,
                                  query_year, repetition) {
  out_path <- tempfile(pattern = "parquetize_", tmpdir = out_dir)
  dir.create(out_path, showWarnings = FALSE, recursive = TRUE)

  pr <- peakRAM::peakRAM(
    tbl_to_pq_parquetize(
      tbl = tbl,
      out_path = out_path,
      max_rows = max_rows
    )
  )

  .benchmark_result(
    method = "parquetize",
    out_path = out_path,
    pr = pr,
    workload = workload,
    label = label,
    query_year = query_year,
    chunk_size = max_rows,
    repetition = repetition
  )
}

.benchmark_result <- function(method, out_path, pr, workload, label, query_year,
                              chunk_size, repetition) {
  rows <- .parquet_row_count(out_path)
  file_size <- .parquet_file_size(out_path)
  schema <- db2pq:::.parquet_schema_text(out_path)
  schema_fingerprint <- db2pq:::.schema_fingerprint(schema)

  data.frame(
    method = method,
    elapsed = pr$Elapsed_Time_sec[[1]],
    total_ram_mib = pr$Total_RAM_Used_MiB[[1]],
    peak_ram_mib = pr$Peak_RAM_Used_MiB[[1]],
    rows = rows,
    file_size = file_size,
    out_path = out_path,
    workload = db2pq:::.as_single_character(workload),
    label = db2pq:::.as_single_character(label),
    query_year = db2pq:::.as_single_integer(query_year),
    chunk_size = db2pq:::.as_single_integer(chunk_size),
    repetition = db2pq:::.as_single_integer(repetition),
    schema = schema,
    schema_fingerprint = schema_fingerprint,
    schema_mismatch = FALSE,
    stringsAsFactors = FALSE
  )
}

.parquet_row_count <- function(path) {
  ds <- arrow::open_dataset(path)
  out <- dplyr::collect(dplyr::summarise(ds, n = dplyr::n()))
  out$n[[1]]
}

.parquet_file_size <- function(path) {
  if (dir.exists(path)) {
    files <- list.files(path, recursive = TRUE, full.names = TRUE)
    return(sum(file.info(files)$size, na.rm = TRUE))
  }

  file.info(path)$size[[1]]
}
