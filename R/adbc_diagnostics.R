#' Report optional ADBC dependency status
#'
#' @return A tibble with installed status and package versions for optional ADBC
#'   packages used by `db2pq`.
#' @export
adbc_diagnostics <- function() {
  packages <- c("adbi", "adbcpostgresql", "adbcdrivermanager", "nanoarrow")
  tibble::tibble(
    package = packages,
    installed = vapply(packages, requireNamespace, logical(1), quietly = TRUE),
    version = vapply(packages, function(pkg) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        return(NA_character_)
      }
      as.character(utils::packageVersion(pkg))
    }, character(1)),
    role = c(
      "DBI adapter for ADBC drivers",
      "ADBC PostgreSQL driver",
      "Optional driver manager backend",
      "Arrow chunk inspection used by ADBC diagnostics"
    )
  )
}
