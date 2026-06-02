# Remove a Parquet file from active or archive storage

Deletes a Parquet file. By default the active file (in the schema
directory) is removed. Set `archive = TRUE` to remove a file from the
archive subdirectory instead.

## Usage

``` r
pq_remove(
  table_name = NULL,
  schema = NULL,
  data_dir = NULL,
  file_name = NULL,
  archive = FALSE,
  archive_dir = "archive"
)
```

## Arguments

- table_name:

  Name of the table. Required if `file_name` is not provided.

- schema:

  Schema name. Required if `file_name` is not provided.

- data_dir:

  Root directory of the Parquet data repository. Defaults to the
  `DATA_DIR` environment variable, with interactive setup when needed.

- file_name:

  Full path to the Parquet file to remove. If provided, `table_name`,
  `schema`, `data_dir`, and `archive` are ignored.

- archive:

  If `TRUE`, resolve the file from the archive subdirectory rather than
  the active schema directory.

- archive_dir:

  Name of the archive subdirectory relative to the schema directory.
  Defaults to `"archive"`.

## Value

Invisibly returns the path of the removed file, or `NULL` if the file
was not found or could not be deleted.

## Details

Either `file_name` or both `table_name` and `schema` must be provided.

## See also

[`pq_archive`](https://iangow.github.io/db2pqr/reference/pq_archive.md),
[`pq_restore`](https://iangow.github.io/db2pqr/reference/pq_restore.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# These examples require an existing local Parquet data repository
pq_remove("company", "comp")
pq_remove("company_20251109T072042Z", "comp", archive = TRUE)
pq_remove(file_name = "~/pq_data/comp/company.parquet")
} # }
```
