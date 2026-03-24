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
#'
#' @return A data frame with one row per benchmarked method.
#' @export
benchmark_tbl_to_pq <- function(tbl,
                                methods = c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"),
                                out_dir = tempdir(),
                                chunk_size = 100000L,
                                parquetize_max_rows = chunk_size,
                                metadata = NULL,
                                col_types = NULL) {
  .require_benchmark_packages(methods)

  sql <- as.character(dbplyr::sql_render(tbl))
  con <- dbplyr::remote_con(tbl)
  methods <- match.arg(methods, c("lazy_tbl_to_pq", "tbl_to_pq", "parquetize"), several.ok = TRUE)

  do.call(rbind, lapply(methods, function(method) {
    switch(method,
      lazy_tbl_to_pq = .benchmark_lazy_tbl_to_pq(
        tbl = tbl,
        out_dir = out_dir,
        chunk_size = chunk_size,
        metadata = metadata,
        col_types = col_types
      ),
      tbl_to_pq = .benchmark_tbl_to_pq_dev(
        tbl = tbl,
        out_dir = out_dir,
        chunk_size = chunk_size,
        metadata = metadata,
        col_types = col_types
      ),
      parquetize = .benchmark_parquetize(
        con = con,
        sql = sql,
        out_dir = out_dir,
        max_rows = parquetize_max_rows
      )
    )
  }))
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

.benchmark_lazy_tbl_to_pq <- function(tbl, out_dir, chunk_size, metadata, col_types) {
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

  .benchmark_result("lazy_tbl_to_pq", out_file, pr)
}

.benchmark_tbl_to_pq_dev <- function(tbl, out_dir, chunk_size, metadata, col_types) {
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

  .benchmark_result("tbl_to_pq", out_file, pr)
}

.benchmark_parquetize <- function(con, sql, out_dir, max_rows) {
  out_path <- tempfile(pattern = "parquetize_", tmpdir = out_dir)
  dir.create(out_path, showWarnings = FALSE, recursive = TRUE)

  pr <- peakRAM::peakRAM(
    parquetize::dbi_to_parquet(
      conn = con,
      sql_query = sql,
      path_to_parquet = out_path,
      max_rows = max_rows
    )
  )

  .benchmark_result("parquetize", out_path, pr)
}

.benchmark_result <- function(method, path, pr) {
  rows <- .parquet_row_count(path)
  file_size <- .parquet_file_size(path)

  data.frame(
    path = method,
    elapsed = pr$Elapsed_Time_sec[[1]],
    total_ram_mib = pr$Total_RAM_Used_MiB[[1]],
    peak_ram_mib = pr$Peak_RAM_Used_MiB[[1]],
    rows = rows,
    file_size = file_size,
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
