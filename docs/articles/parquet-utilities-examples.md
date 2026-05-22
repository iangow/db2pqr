# Examples Demonstrating Parquet Utility Functions

This page groups together examples for the Parquet utility helpers:
[`pq_last_modified()`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md),
[`pq_archive()`](https://iangow.github.io/db2pqr/reference/pq_archive.md),
[`pq_restore()`](https://iangow.github.io/db2pqr/reference/pq_restore.md),
and
[`pq_remove()`](https://iangow.github.io/db2pqr/reference/pq_remove.md).

The goal is different from the API reference. The API pages document
arguments and return values. This page shows how the utilities fit
together in the practical workflow of maintaining a local Parquet
repository.

## When to Use This Page

Use these helpers when you already have a Parquet repository and want
to:

- inspect what is in it
- check which vintage of a table you currently have
- archive a current active file before replacing it
- restore an archived vintage
- remove active or archived files explicitly

The related API pages are:

- [`pq_last_modified()`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md)
- [`pq_archive()`](https://iangow.github.io/db2pqr/reference/pq_archive.md)
- [`pq_restore()`](https://iangow.github.io/db2pqr/reference/pq_restore.md)
- [`pq_remove()`](https://iangow.github.io/db2pqr/reference/pq_remove.md)

## Setup

Load the packages used in the examples:

``` r

library(db2pq)
library(dplyr)
```

Set `DATA_DIR` to the Parquet repository you want to inspect:

``` r

Sys.setenv(DATA_DIR = "~/Dropbox/pq_data")
```

The examples below are rendered from a local Parquet repository. The
examples that move files run only during a local `pkgdown` render when
the repository contains `comp.company` and `crsp.dsi`.

## Inspect a Schema Directory

Use
[`pq_last_modified()`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md)
without `table_name` to summarize the active Parquet files in a schema:

``` r

pq_last_modified(schema = "comp") |>
  select(file_name, table, last_mod) |>
  head(10)
#> # A tibble: 10 × 3
#>    file_name   table       last_mod           
#>    <chr>       <chr>       <dttm>             
#>  1 aco_pnfnda  aco_pnfnda  2026-05-21 06:00:00
#>  2 adsprate    adsprate    2026-04-09 06:00:00
#>  3 co_adesind  co_adesind  2026-04-09 06:00:00
#>  4 co_afnd2    co_afnd2    2026-04-09 06:00:00
#>  5 co_filedate co_filedate 2026-04-09 06:00:00
#>  6 co_hgic     co_hgic     2026-04-09 06:00:00
#>  7 co_ifndq    co_ifndq    2026-04-09 06:00:00
#>  8 company     company     2026-05-22 06:00:00
#>  9 funda       funda       2026-05-21 06:00:00
#> 10 funda_fncd  funda_fncd  2026-05-21 06:00:00
```

If you want to inspect archived files instead:

``` r

pq_last_modified(schema = "comp", archive = TRUE) |>
  select(file_name, table, last_mod) |>
  head(10)
#> # A tibble: 10 × 3
#>    file_name                   table      last_mod           
#>    <chr>                       <chr>      <dttm>             
#>  1 aco_pnfnda_20260330T060000Z aco_pnfnda 2026-03-30 06:00:00
#>  2 company_20260105T070000Z    company    2026-01-05 07:00:00
#>  3 company_20260107T070000Z    company    2026-01-07 07:00:00
#>  4 company_20260209T070000Z    company    2026-02-09 07:00:00
#>  5 company_20260218T070000Z    company    2026-02-18 07:00:00
#>  6 company_20260224T070000Z    company    2026-02-24 07:00:00
#>  7 company_20260225T000000Z    company    2026-02-24 07:00:00
#>  8 company_20260225T070000Z    company    2026-02-25 07:00:00
#>  9 company_20260226T070000Z    company    2026-02-26 07:00:00
#> 10 company_20260303T070000Z    company    2026-03-03 07:00:00
```

If your project uses a repository outside the default `DATA_DIR`, pass
`data_dir` explicitly:

``` r

pq_last_modified(schema = "crsp", data_dir = Sys.getenv("DATA_DIR")) |>
  select(file_name, table, last_mod) |>
  head(5)
#> # A tibble: 5 × 3
#>   file_name        table            last_mod           
#>   <chr>            <chr>            <dttm>             
#> 1 ccmxpf_linktable ccmxpf_linktable 2026-02-06 07:00:00
#> 2 ccmxpf_lnkhist   ccmxpf_lnkhist   2026-02-06 07:00:00
#> 3 ccmxpf_lnkused   ccmxpf_lnkused   2026-02-06 07:00:00
#> 4 comphist         comphist         2026-02-06 07:00:00
#> 5 dse              dse              2025-02-08 07:00:00
```

## Check the Current Active Vintage

With `table_name` and `schema`,
[`pq_last_modified()`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md)
returns the raw embedded `last_modified` metadata string for the active
file:

``` r

pq_last_modified(table_name = "dsi", schema = "crsp")
#> [1] "Stock - Market Indexes Daily NYSE/AMEX/NASDAQ/ARCA (Updated 2025-02-08)"
```

This is often the fastest way to confirm what vintage a local Parquet
file represents before starting analysis.

## Inspect Archived Vintages

If you archive replaced files, you can ask for the archived versions of
a table:

``` r

pq_last_modified(table_name = "company", schema = "comp", archive = TRUE) |>
  select(file_name, table, last_mod, last_mod_str) |>
  tail(10)
#> # A tibble: 10 × 4
#>    file_name                table   last_mod            last_mod_str            
#>    <chr>                    <chr>   <dttm>              <chr>                   
#>  1 company_20260225T000000Z company 2026-02-24 07:00:00 Company (Updated 2026-0…
#>  2 company_20260225T070000Z company 2026-02-25 07:00:00 Company (Updated 2026-0…
#>  3 company_20260226T070000Z company 2026-02-26 07:00:00 Company (Updated 2026-0…
#>  4 company_20260303T070000Z company 2026-03-03 07:00:00 Company (Updated 2026-0…
#>  5 company_20260315T060000Z company 2026-03-15 06:00:00 Company (Updated 2026-0…
#>  6 company_20260322T060000Z company 2026-03-22 06:00:00 Company (Updated 2026-0…
#>  7 company_20260323T060000Z company 2026-03-23 06:00:00 Company (Updated 2026-0…
#>  8 company_20260331T060000Z company 2026-03-31 06:00:00 Company (Updated 2026-0…
#>  9 company_20260402T060000Z company 2026-04-02 06:00:00 Company (Updated 2026-0…
#> 10 company_20260407T060000Z company 2026-04-07 06:00:00 Company (Updated 2026-0…
```

That returns a table-like summary of the archived vintages for the
requested dataset. To inspect archived files for a whole schema, use
`schema` without `table_name`:

``` r

pq_last_modified(schema = "comp", archive = TRUE) |>
  select(file_name, table, last_mod) |>
  head(10)
#> # A tibble: 10 × 3
#>    file_name                   table      last_mod           
#>    <chr>                       <chr>      <dttm>             
#>  1 aco_pnfnda_20260330T060000Z aco_pnfnda 2026-03-30 06:00:00
#>  2 company_20260105T070000Z    company    2026-01-05 07:00:00
#>  3 company_20260107T070000Z    company    2026-01-07 07:00:00
#>  4 company_20260209T070000Z    company    2026-02-09 07:00:00
#>  5 company_20260218T070000Z    company    2026-02-18 07:00:00
#>  6 company_20260224T070000Z    company    2026-02-24 07:00:00
#>  7 company_20260225T000000Z    company    2026-02-24 07:00:00
#>  8 company_20260225T070000Z    company    2026-02-25 07:00:00
#>  9 company_20260226T070000Z    company    2026-02-26 07:00:00
#> 10 company_20260303T070000Z    company    2026-03-03 07:00:00
```

## Archive the Currently Active File

You can archive a file manually even outside an update workflow:

``` r

pq_archive(table_name = "company", schema = "comp")
```

Or archive an exact file path. During the live render, this moves the
current `comp.company` file into its archive directory:

``` r

company_archive <- pq_archive(file_name = company_file)
basename(company_archive)
#> [1] "company_20260522T060000Z.parquet"
```

This is useful when you want to preserve the current active vintage
before running an experimental refresh or downstream transformation.

## Restore an Archived Vintage

To promote an archived file back into the active schema directory:

``` r

restored_company <- pq_restore(
  tools::file_path_sans_ext(basename(company_archive)),
  "comp",
  archive = FALSE
)
basename(restored_company)
#> [1] "company.parquet"
```

The archived basename may include or omit the `.parquet` suffix. If an
active destination file already exists,
[`pq_restore()`](https://iangow.github.io/db2pqr/reference/pq_restore.md)
can archive that file first with its default `archive = TRUE`.

``` r

pq_last_modified(table_name = "company", schema = "comp")
#> [1] "Company (Updated 2026-05-22)"
```

## Remove a File Explicitly

Use
[`pq_remove()`](https://iangow.github.io/db2pqr/reference/pq_remove.md)
when you want to delete an active or archived file rather than archive
it. The removal examples below are shown but not run during the
documentation build. For an active file:

``` r

pq_remove(table_name = "dsi", schema = "crsp")
```

To remove an archived file:

``` r

pq_remove(
  table_name = "company_20260407T060000Z",
  schema = "comp",
  archive = TRUE
)
```

Or remove a file by exact path:

``` r

pq_remove(file_name = company_archive)
```

## Related Pages

- Data management article
- WRDS to Parquet article
- PostgreSQL to Parquet article
- Parquet file utility reference pages
