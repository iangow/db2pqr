#' db2pq: Export Database Tables to Parquet
#'
#' Tools for exporting PostgreSQL and WRDS data to Apache Parquet files,
#' managing a local Parquet data repository, and checking whether local files
#' are current against source table metadata.
#'
#' @section Configuration:
#' `DATA_DIR` controls the default root directory for Parquet output;
#' \code{\link{db2pq_data_dir}} can guide interactive first-time setup. WRDS
#' helpers resolve usernames from explicit arguments, `WRDS_ID`, `WRDS_USER`,
#' and then the credential store used by the `wrds` package.
#'
#' @keywords internal
"_PACKAGE"
