# WRDS to Parquet

This is the place to start when your source data live in the WRDS
PostgreSQL database and you want local Parquet files that can be reused
across projects.

`db2pq` writes files using the layout:

``` text
<DATA_DIR>/<schema>/<table>.parquet
```

For example, `wrds_update_pq("company", "comp")` writes
`<DATA_DIR>/comp/company.parquet` unless you provide `data_dir` or
`out_file` explicitly.

## Main Helpers

The main helper on this path is
[`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md).
It downloads a WRDS PostgreSQL table as a Parquet file and skips the
download when the existing local file already appears current.

Related helpers are:

- [`wrds_sql_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_sql_to_pq.md)
  exports a custom SQL query against WRDS into the standard Parquet
  layout.
- [`wrds_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_schema_to_pq.md)
  updates all or selected tables in a WRDS schema.
- [`pq_last_modified()`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md),
  [`pq_archive()`](https://iangow.github.io/db2pqr/reference/pq_archive.md),
  and
  [`pq_restore()`](https://iangow.github.io/db2pqr/reference/pq_restore.md)
  inspect and manage local Parquet files after they have been created.

## Setup

Set `DATA_DIR` to the local repository where you want Parquet files to
live:

``` r

Sys.setenv(DATA_DIR = "~/Dropbox/pq_data")
```

For WRDS credentials, `db2pq` can use credentials configured by the
`wrds` package, and it also provides helpers for checking username and
connection settings:

``` r

Sys.setenv(WRDS_ID = "your_wrds_id")
wrds_get_username()
wrds_conninfo(format = "uri")
wrds_check_credentials()
```

For compatibility with Tidy Finance-style setups, `WRDS_USER` is also
recognized. See the authentication article for more detail. SSH access
is only needed when using SAS metadata with `use_sas = TRUE`; see the
[WRDS SSH setup
article](https://iangow.github.io/db2pqr/articles/wrds-ssh.md) for that
separate setup.

## Using `wrds_update_pq()`

### An Illustrative First Run

A good first table is `comp.company`, which is small enough for a quick
test:

``` r

library(db2pq)

wrds_update_pq("company", "comp")
```

When this article is rendered locally with WRDS credentials, the same
example runs against a temporary documentation repository rather than
the main `DATA_DIR`.

``` r

library(db2pq)

wrds_id <- wrds_get_username()
company_file <- wrds_update_pq(
  "company",
  "comp",
  data_dir = doc_data_dir,
  wrds_id = wrds_id
)
#> Updated comp.company is available.
#> Beginning file download at 2026-05-23 15:32:18 UTC.
#> Completed file download at 2026-05-23 15:32:19 UTC.
file.path("<temporary data repository>", "comp", basename(company_file))
#> [1] "<temporary data repository>/comp/company.parquet"
```

Typical output from a first run looks like:

``` text
Updated comp.company is available.
Beginning file download at 2026-04-09 17:12:41 UTC.
Completed file download at 2026-04-09 17:12:46 UTC.
```

Running the same command later should usually skip the download if WRDS
metadata indicate that the local file is already current.

``` r

wrds_update_pq("company", "comp", data_dir = doc_data_dir, wrds_id = wrds_id)
#> comp.company already up to date.
```

The resulting file is an ordinary Parquet file. It can be inspected with
`arrow`, `dplyr`, DuckDB, Python, or any other tool that reads Parquet.

``` r

dplyr::select(
  arrow::read_parquet(company_file),
  gvkey, conm, sic
) |>
  head()
#> # A tibble: 6 × 3
#>   gvkey  conm                    sic  
#>   <chr>  <chr>                   <chr>
#> 1 001000 A & E PLASTIK PAK INC   3089 
#> 2 001001 A & M FOOD SERVICES INC 5812 
#> 3 001002 AAI CORP                3825 
#> 4 001003 A.A. IMPORTING CO INC   5712 
#> 5 001004 AAR CORP                5080 
#> 6 001005 A.B.A. INDUSTRIES INC   3724
```

### Identifying Table Names and Schemas

WRDS uses schema names such as `crsp`, `comp`, `audit`, and `bank`. If
existing code refers to `comp.funda`, then the schema is `comp` and the
table is `funda`:

``` r

wrds_update_pq("funda", "comp")
```

You can list tables in a WRDS schema from R:

``` r

wrds_get_tables("crsp")
wrds_get_tables("comp", views = TRUE)
```

Use the PostgreSQL schema name exposed by WRDS, which can differ from a
short product name or SAS library name. For example, the Fama-French
PostgreSQL base-table schema is `ff_all`, so a schema download starts
with:

``` r

wrds_schema_to_pq("ff_all")
```

Schemas containing views need `views = TRUE` during discovery. For
example, relations exposed through the Fama-French `ff` schema can be
processed with:

``` r

wrds_schema_to_pq("ff", views = TRUE)
```

During a local site render, this page can show the first few tables
returned from WRDS:

``` r

head(wrds_get_tables("comp", wrds_id = wrds_id))
#> character(0)
```

The WRDS web query interface can also be useful for identifying the
underlying schema and table behind a dataset, but once the table is
known, script the download with `db2pq` so it is reproducible.

## Cleaning and Shaping Data

### Correcting Data Types

Some WRDS columns are stored in PostgreSQL with a type that is not ideal
for analysis. For example, link-history identifiers in
`crsp.ccmxpf_lnkhist` are often better represented as integers:

``` r

wrds_update_pq(
  "ccmxpf_lnkhist",
  "crsp",
  col_types = list(lpermno = "int32", lpermco = "int32")
)
```

### Setting Time Zones

WRDS PostgreSQL often stores timestamps without time-zone information.
The `tz` argument tells `db2pq` how to interpret those values before
writing normalized timestamps to Parquet.

For FFIEC Call Report data in the WRDS `bank` schema, timestamps should
be interpreted as US Eastern time:

``` r

wrds_update_pq(
  "wrds_call_rcfa_1",
  "bank",
  tz = "America/New_York"
)
```

Some timestamp-like fields are stored as strings. Use `col_types` with
`tz` to coerce selected fields:

``` r

wrds_update_pq(
  "feed03_audit_fees",
  "audit",
  keep = c("auditor_fkey", "file_accepted"),
  col_types = list(
    auditor_fkey = "int32",
    file_accepted = "timestamp"
  ),
  tz = "America/New_York",
  force = TRUE
)
```

## Working With Large Tables

### Dropping Unwanted Variables

Some WRDS tables contain blocks of columns that are not needed for most
analyses. The `drop` argument removes columns whose names match one or
more regular expressions:

``` r

wrds_update_pq(
  "feed02_auditor_changes",
  "audit",
  drop = "^(match|prior|closest)"
)
```

### Selecting Only Certain Variables

Use `keep` when you want a small subset of columns:

``` r

wrds_update_pq(
  "dsf",
  "crsp",
  keep = c("permno", "date", "ret")
)
```

### Renaming Variables

Use `rename` to write output columns with preferred names:

``` r

wrds_update_pq(
  "company",
  "comp",
  keep = c("gvkey", "conm", "sic"),
  rename = c(conm = "company_name")
)
```

For a small documentation example, the renamed file can be written to
the temporary repository:

``` r

renamed_company_file <- wrds_update_pq(
  "company",
  "comp",
  data_dir = doc_data_dir,
  alt_table_name = "company_names",
  keep = c("gvkey", "conm", "sic"),
  rename = c(conm = "company_name"),
  wrds_id = wrds_id,
  force = TRUE
)
#> Forcing update based on user request.
#> Beginning file download at 2026-05-23 15:32:21 UTC.
#> Completed file download at 2026-05-23 15:32:21 UTC.

dplyr::select(
  arrow::read_parquet(renamed_company_file),
  gvkey, company_name, sic
) |>
  head()
#> # A tibble: 6 × 3
#>   gvkey  company_name            sic  
#>   <chr>  <chr>                   <chr>
#> 1 001000 A & E PLASTIK PAK INC   3089 
#> 2 001001 A & M FOOD SERVICES INC 5812 
#> 3 001002 AAI CORP                3825 
#> 4 001003 A.A. IMPORTING CO INC   5712 
#> 5 001004 AAR CORP                5080 
#> 6 001005 A.B.A. INDUSTRIES INC   3724
```

If `rename` and `col_types` are used together, names in `col_types`
refer to the output names after renaming:

``` r

wrds_update_pq(
  "ccmxpf_lnkhist",
  "crsp",
  rename = c(lpermno = "permno_link"),
  col_types = list(permno_link = "int32")
)
```

### Selecting Only Certain Rows

Use `where` for SQL row filters. For Compustat annual fundamentals, a
common filter is the standard industrial format, standard data format,
consolidated, domestic-population set:

``` r

wrds_update_pq(
  "funda",
  "comp",
  where = paste(
    "indfmt = 'INDL'",
    "AND datafmt = 'STD'",
    "AND consol = 'C'",
    "AND popsrc = 'D'"
  )
)
```

### Limiting Rows for a Test Download

Use `obs` to test a download before running the full export:

``` r

wrds_update_pq("funda", "comp", obs = 1000)
```

Combining `keep`, `where`, and `obs` is a useful way to produce a small
local file while developing a refresh script:

``` r

funda_sample_file <- wrds_update_pq(
  "funda",
  "comp",
  data_dir = doc_data_dir,
  alt_table_name = "funda_sample",
  keep = c("gvkey", "datadate", "fyear", "at", "sale"),
  where = paste(
    "indfmt = 'INDL'",
    "AND datafmt = 'STD'",
    "AND consol = 'C'",
    "AND popsrc = 'D'"
  ),
  obs = 1000,
  wrds_id = wrds_id,
  force = TRUE
)
#> Forcing update based on user request.
#> Beginning file download at 2026-05-23 15:32:22 UTC.
#> Completed file download at 2026-05-23 15:32:23 UTC.

dplyr::glimpse(arrow::read_parquet(funda_sample_file))
#> Rows: 1,000
#> Columns: 29
#> $ gvkey    <chr> "001000", "001000", "001000", "001000", "001000", "001000", "…
#> $ datadate <date> 1961-12-31, 1962-12-31, 1963-12-31, 1964-12-31, 1965-12-31, …
#> $ fyear    <int> 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1…
#> $ datafmt  <chr> "STD", "STD", "STD", "STD", "STD", "STD", "STD", "STD", "STD"…
#> $ apdedate <date> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, …
#> $ fdate    <date> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, …
#> $ pdate    <date> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, …
#> $ at       <dbl> NA, NA, NA, 1.416, 2.310, 2.430, 2.456, 5.922, 28.712, 33.450…
#> $ batr     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fatb     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fatc     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fatd     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fate     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fatl     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fatn     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fato     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ fatp     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ iatci    <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ iati     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ iatmi    <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ lcat     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ nat      <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ npat     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ patr     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ rati     <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ sale     <dbl> 0.900, 1.600, 1.457, 2.032, 1.688, 4.032, 3.594, 7.400, 37.39…
#> $ salepfc  <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ salepfp  <dbl> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, N…
#> $ costat   <chr> "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "…
```

## Update Logic

### Forcing an Update

Use `force = TRUE` when you want to re-download even if local metadata
suggest the file is current. This is useful after changing `keep`,
`drop`, `rename`, `col_types`, or `tz`.

``` r

wrds_update_pq("funda", "comp", force = TRUE)
```

### Using SAS Metadata

By default,
[`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)
compares the local Parquet metadata with WRDS PostgreSQL table comments.
Some WRDS PostgreSQL tables do not have useful comments. In those cases,
`use_sas = TRUE` can retrieve update metadata from the corresponding
WRDS SAS dataset.

Some schemas have different names on the PostgreSQL and SAS sides. For
example, RavenPack’s PostgreSQL schema can differ from its SAS library:

``` r

wrds_update_pq(
  "rpa_entity_mappings",
  "ravenpack_common",
  use_sas = TRUE,
  sas_schema = "rpa"
)
```

This path requires SSH access to the WRDS SAS server. See the [WRDS SSH
setup article](https://iangow.github.io/db2pqr/articles/wrds-ssh.md).

### Archiving Data When Updating

Use `archive = TRUE` when you want replaced files moved into the
schema’s archive directory before the new file is written:

``` r

wrds_update_pq("company", "comp", archive = TRUE)
pq_last_modified("company", "comp", archive = TRUE)
```

Archives live under:

``` text
<DATA_DIR>/<schema>/archive/
```

For documentation and testing, archive examples should use a temporary
data repository. This keeps the real local Parquet repository unchanged
while still exercising the same file-management code.

``` r

archived_company <- pq_archive("company", "comp", data_dir = doc_data_dir)
basename(archived_company)
#> [1] "company_20260523T060000Z.parquet"

pq_last_modified("company", "comp", data_dir = doc_data_dir, archive = TRUE) |>
  dplyr::select(file_name, last_mod)
#> # A tibble: 1 × 2
#>   file_name                last_mod           
#>   <chr>                    <dttm>             
#> 1 company_20260523T060000Z 2026-05-23 06:00:00
```

The archived copy can be restored, again without touching the main
`DATA_DIR`.

``` r

pq_restore(
  tools::file_path_sans_ext(basename(archived_company)),
  "comp",
  data_dir = doc_data_dir,
  archive = FALSE
)

pq_last_modified("company", "comp", data_dir = doc_data_dir)
#> [1] "Company (Updated 2026-05-23)"
```

## Custom SQL

Use
[`wrds_sql_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_sql_to_pq.md)
when the object you want is better expressed as a query than as a full
table export:

``` r

wrds_sql_to_pq(
  "SELECT permno, date, ret FROM crsp.dsf WHERE date >= '2024-01-01'",
  table_name = "dsf_recent",
  schema = "crsp"
)
```

For a small query result, use `LIMIT` and write to the temporary
documentation repository:

``` r

sql_file <- wrds_sql_to_pq(
  paste(
    "SELECT gvkey, datadate, fyear, at, sale",
    "FROM comp.funda",
    "WHERE indfmt = 'INDL'",
    "AND datafmt = 'STD'",
    "AND consol = 'C'",
    "AND popsrc = 'D'",
    "ORDER BY gvkey, datadate",
    "LIMIT 20"
  ),
  table_name = "funda_sql_sample",
  schema = "comp",
  data_dir = doc_data_dir,
  wrds_id = wrds_id,
  archive = TRUE
)

arrow::read_parquet(sql_file)
#> # A tibble: 20 × 5
#>    gvkey  datadate   fyear    at  sale
#>    <chr>  <date>     <int> <dbl> <dbl>
#>  1 001000 1961-12-31  1961 NA     0.9 
#>  2 001000 1962-12-31  1962 NA     1.6 
#>  3 001000 1963-12-31  1963 NA     1.46
#>  4 001000 1964-12-31  1964  1.42  2.03
#>  5 001000 1965-12-31  1965  2.31  1.69
#>  6 001000 1966-12-31  1966  2.43  4.03
#>  7 001000 1967-12-31  1967  2.46  3.59
#>  8 001000 1968-12-31  1968  5.92  7.4 
#>  9 001000 1969-12-31  1969 28.7  37.4 
#> 10 001000 1970-12-31  1970 33.4  45.3 
#> 11 001000 1971-12-31  1971 29.3  47.0 
#> 12 001000 1972-12-31  1972 19.9  34.4 
#> 13 001000 1973-12-31  1973 21.8  37.8 
#> 14 001000 1974-12-31  1974 25.6  50.3 
#> 15 001000 1975-12-31  1975 23.9  51.2 
#> 16 001000 1976-12-31  1976 38.6  66.4 
#> 17 001000 1977-12-31  1977 44.0  77.9 
#> 18 001001 1978-12-31  1978 NA    NA   
#> 19 001001 1979-12-31  1979 NA    NA   
#> 20 001001 1980-12-31  1980 NA    NA
```

## Related Pages

- [`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)
- [`wrds_sql_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_sql_to_pq.md)
- [`wrds_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_schema_to_pq.md)
- [`pq_last_modified()`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md)
- [`pq_archive()`](https://iangow.github.io/db2pqr/reference/pq_archive.md)
- Authentication article
- Data management article
