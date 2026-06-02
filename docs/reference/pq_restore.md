# Restore an archived Parquet file to the active schema directory

Moves an archived Parquet file back into the schema directory, stripping
the timestamp suffix to recover the original filename. If a current file
already exists at the destination, it is archived first (when
`archive = TRUE`) or the function stops (when `archive = FALSE`).

## Usage

``` r
pq_restore(
  file_basename,
  schema,
  data_dir = NULL,
  archive = TRUE,
  archive_dir = "archive"
)
```

## Arguments

- file_basename:

  Basename of the archived file (with or without the `.parquet`
  extension), e.g. `"company_20251109T072042Z"`.

- schema:

  Schema name.

- data_dir:

  Root directory of the Parquet data repository. Defaults to the
  `DATA_DIR` environment variable, with interactive setup when needed.

- archive:

  If `TRUE` (default), any existing file at the destination is archived
  before the restore. If `FALSE`, the function stops if the destination
  already exists.

- archive_dir:

  Name of the archive subdirectory. Defaults to `"archive"`.

## Value

Invisibly returns the path to the restored file, or `NULL` on failure.

## See also

[`pq_archive`](https://iangow.github.io/db2pqr/reference/pq_archive.md),
[`pq_last_modified`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Requires an existing archived file in a local Parquet data repository
pq_restore("company_20251109T072042Z", "comp")
} # }
```
