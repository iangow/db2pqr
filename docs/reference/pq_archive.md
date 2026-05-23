# Archive a Parquet file into the archive subdirectory

Moves a Parquet file into an archive subdirectory, appending a timestamp
suffix derived from the file's `last_modified` metadata. The archived
filename takes the form `<table>_<YYYYMMDDTHHMMSSz>.parquet`.

## Usage

``` r
pq_archive(
  table_name = NULL,
  schema = NULL,
  data_dir = db2pq_data_dir(),
  file_name = NULL,
  archive_dir = "archive"
)
```

## Arguments

- table_name:

  Name of the table. Required if `file_name` is not provided.

- schema:

  Schema name. Required if `file_name` is not provided.

- data_dir:

  Root directory of the Parquet data repository. Defaults to
  [`db2pq_data_dir`](https://iangow.github.io/db2pqr/reference/db2pq_data_dir.md).

- file_name:

  Full path to the Parquet file to archive. If provided, `table_name`,
  `schema`, and `data_dir` are ignored.

- archive_dir:

  Name of the archive subdirectory relative to the schema directory.
  Defaults to `"archive"`.

## Value

Invisibly returns the path to the archived file, or `NULL` if the source
file does not exist.

## Details

Either `file_name` or both `table_name` and `schema` must be provided.

## See also

[`pq_restore`](https://iangow.github.io/db2pqr/reference/pq_restore.md),
[`wrds_update_pq`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)

## Examples

``` r
if (FALSE) { # \dontrun{
pq_archive("company", "comp")
pq_archive(file_name = "~/pq_data/comp/company.parquet")
} # }
```
