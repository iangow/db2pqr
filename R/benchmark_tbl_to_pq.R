#' Export a lazy `dbplyr` table to Parquet via `parquetize`
#'
#' Renders a lazy `dbplyr` query to SQL, retrieves the backing DBI connection,
#' and writes the result through [parquetize::dbi_to_parquet()].
#'
#' @param tbl A lazy table backed by `dbplyr`.
#' @param out_path Directory where the Parquet dataset should be written.
#' @param max_rows Maximum number of rows per fetched chunk.
#'
#' @return Invisibly returns `out_path`.
#' @export
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

#' Benchmark Parquet export paths for a lazy `dbplyr` table
#'
#' Runs one or more table-to-Parquet export methods serially on the same lazy
#' `dbplyr` query and returns a comparison table with elapsed time, memory use,
#' output row count, and file size.
#'
#' Supported methods are:
#' - `"lazy_tbl_to_pq"` for the package's existing DBI/data-frame path
#' - `"tbl_to_pq"` for the experimental ADBC/Arrow path
#' - `"parquetize"` for [parquetize::dbi_to_parquet()]
#'
#' The methods are always run in series to avoid overlapping database, network,
#' or disk load.
#'
#' @param tbl A lazy table backed by `dbplyr`.
#' @param methods Character vector of methods to benchmark. Defaults to all
#'   supported methods.
#' @param out_dir Directory used for temporary output files.
#' @param chunk_size Chunk size used by [lazy_tbl_to_pq()] and [tbl_to_pq()].
#' @param parquetize_max_rows Chunk size used by [parquetize::dbi_to_parquet()].
#'   Defaults to `chunk_size`.
#' @param metadata Optional named list of schema metadata passed to
#'   [lazy_tbl_to_pq()] and [tbl_to_pq()].
#' @param col_types Optional named list of Arrow type overrides passed to
#'   [lazy_tbl_to_pq()] and [tbl_to_pq()].
#' @param workload Optional workload name recorded in the benchmark result.
#' @param label Optional descriptive label recorded in the benchmark result.
#' @param query_year Optional year recorded in the benchmark result.
#' @param repetition Optional repetition index recorded in the benchmark result.
#'
#' @return A data frame with one row per benchmarked method.
#' @export
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

  .annotate_schema_mismatch(out)
}

#' Build standard PostgreSQL-to-Parquet benchmark workloads
#'
#' Creates a named list of lazy `dbplyr` queries for the default benchmark
#' backbone using a fixed year window.
#'
#' @param con A live DBI connection.
#' @param year Optional calendar year for the benchmark window. If `NULL`, use
#'   the most recent complete calendar year.
#' @param span_years Number of years to include in the benchmark window.
#'
#' @return A named list of lazy `dbplyr` queries.
#' @export
build_benchmark_workloads <- function(con, year = NULL, span_years = 1L) {
  if (!requireNamespace("dbplyr", quietly = TRUE) ||
      !requireNamespace("dplyr", quietly = TRUE)) {
    stop(
      "Packages `dbplyr` and `dplyr` must be installed to use `build_benchmark_workloads()`."
    )
  }

  span_years <- .validate_positive_count(span_years, "span_years")
  year <- .resolve_benchmark_year(year)
  start_date <- as.Date(sprintf("%04d-01-01", year))
  end_date <- as.Date(sprintf("%04d-01-01", year + span_years))

  list(
    crsp_dsf = dplyr::filter(
      dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsf")),
      .data$date >= !!start_date,
      .data$date < !!end_date
    ),
    comp_funda = dplyr::filter(
      dplyr::tbl(con, DBI::Id(schema = "comp", table = "funda")),
      .data$datadate >= !!start_date,
      .data$datadate < !!end_date
    ),
    crsp_dsi = dplyr::filter(
      dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsi")),
      .data$date >= !!start_date,
      .data$date < !!end_date
    )
  )
}

#' Run repeated PostgreSQL-to-Parquet benchmarks for standard workloads
#'
#' Benchmarks one or more workload queries serially and returns both per-run
#' results and median summaries.
#'
#' @param workloads Named list of lazy `dbplyr` tables or queries.
#' @param methods Character vector of methods to benchmark.
#' @param repeats Number of repetitions per workload.
#' @param out_dir Directory used for temporary output files.
#' @param chunk_size Chunk size used by [lazy_tbl_to_pq()] and [tbl_to_pq()].
#' @param parquetize_max_rows Chunk size used by [parquetize::dbi_to_parquet()].
#'   Defaults to `chunk_size`.
#' @param metadata Optional named list of schema metadata passed to
#'   [lazy_tbl_to_pq()] and [tbl_to_pq()].
#' @param col_types Optional named list of Arrow type overrides passed to
#'   [lazy_tbl_to_pq()] and [tbl_to_pq()].
#' @param year Optional year recorded in the benchmark result.
#' @param label Optional descriptive label recorded in the benchmark result.
#'
#' @return A list containing `runs` and `summary` data frames.
#' @export
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
  repeats <- .validate_positive_count(repeats, "repeats")

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
    summary = .summarize_benchmark_runs(runs)
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
    lazy_tbl_to_pq(
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
    tbl_to_pq(
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
  schema <- .parquet_schema_text(out_path)
  schema_fingerprint <- .schema_fingerprint(schema)

  data.frame(
    method = method,
    elapsed = pr$Elapsed_Time_sec[[1]],
    total_ram_mib = pr$Total_RAM_Used_MiB[[1]],
    peak_ram_mib = pr$Peak_RAM_Used_MiB[[1]],
    rows = rows,
    file_size = file_size,
    out_path = out_path,
    workload = .as_single_character(workload),
    label = .as_single_character(label),
    query_year = .as_single_integer(query_year),
    chunk_size = .as_single_integer(chunk_size),
    repetition = .as_single_integer(repetition),
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

.parquet_schema_text <- function(path) {
  schema <- arrow::open_dataset(path)$schema
  .schema_text_without_metadata(schema)
}

.schema_fingerprint <- function(schema_text) {
  if (is.na(schema_text) || !nzchar(schema_text)) {
    return(NA_character_)
  }

  tmp <- tempfile("schema_", fileext = ".txt")
  on.exit(unlink(tmp), add = TRUE)
  writeLines(schema_text, tmp, useBytes = TRUE)
  unname(tools::md5sum(tmp)[[1]])
}

.schema_text_without_metadata <- function(schema) {
  if (!is.null(schema$metadata) && length(schema$metadata) > 0L) {
    schema <- schema$WithMetadata(stats::setNames(character(), character()))
  }

  schema$ToString()
}

.annotate_schema_mismatch <- function(results) {
  if (nrow(results) == 0L || !"schema_fingerprint" %in% names(results)) {
    return(results)
  }

  fingerprints <- unique(stats::na.omit(results$schema_fingerprint))
  results$schema_mismatch <- length(fingerprints) > 1L
  results
}

.summarize_benchmark_runs <- function(runs) {
  if (nrow(runs) == 0L) {
    return(runs)
  }

  groups <- split(
    runs,
    interaction(runs$workload, runs$method, runs$query_year, runs$chunk_size, runs$label,
      drop = TRUE
    )
  )

  out <- lapply(groups, function(df) {
    first <- df[1, , drop = FALSE]
    data.frame(
      workload = first$workload,
      method = first$method,
      label = first$label,
      query_year = first$query_year,
      chunk_size = first$chunk_size,
      runs = nrow(df),
      median_elapsed = stats::median(df$elapsed),
      median_total_ram_mib = stats::median(df$total_ram_mib),
      median_peak_ram_mib = stats::median(df$peak_ram_mib),
      rows = first$rows,
      file_size = first$file_size,
      schema_fingerprint = first$schema_fingerprint,
      schema_mismatch = any(df$schema_mismatch),
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, out)
}

.validate_positive_count <- function(x, arg) {
  if (length(x) != 1L || is.na(x) || x < 1L || trunc(x) != x) {
    stop("`", arg, "` must be a positive whole number.", call. = FALSE)
  }

  as.integer(x)
}

.resolve_benchmark_year <- function(year) {
  if (is.null(year) || is.na(year)) {
    return(as.integer(format(Sys.Date(), "%Y")) - 1L)
  }

  .validate_positive_count(year, "year")
}

.as_single_character <- function(x) {
  if (is.null(x) || length(x) == 0L || is.na(x)) {
    return(NA_character_)
  }

  as.character(x[[1]])
}

.as_single_integer <- function(x) {
  if (is.null(x) || length(x) == 0L || is.na(x)) {
    return(NA_integer_)
  }

  as.integer(x[[1]])
}
