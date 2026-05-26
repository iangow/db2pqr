# WRDS to Parquet

``` r

library(db2pq)
```

## Overview

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

## Using `wrds_update_pq()`

### An illustrative first run

A good table to start with is `comp.company`, which is small enough for
testing:

``` r

wrds_update_pq("company", "comp")
#> Updated comp.company is available.
#> Beginning file download at 2026-05-26 13:55:34 UTC.
#> Completed file download at 2026-05-26 13:55:35 UTC.
```

If you run the function above and have not set up your details for a
WRDS connection, you will be asked to provide these (e.g., your WRDS
username and password). More discussion on this topic can be found in
the article on
[authentication](https://iangow.github.io/db2pqr/articles/authentication.md).

In addition, if you have not established `DATA_DIR`, as discussed in the
article on [data
management](https://iangow.github.io/db2pqr/articles/data-management.md),
you will be asked to provide that.

Running the same command later should usually skip the download if WRDS
metadata indicate that the local file is already current.

``` r

company_file <- wrds_update_pq("company", "comp")
#> comp.company already up to date.
```

The resulting file is an ordinary Parquet file. It can be inspected with
`arrow`, `dplyr`, DuckDB, Python, or any other tool that reads Parquet.

``` r

library(arrow)
library(dplyr, warn.conflicts = FALSE)
```

``` r

read_parquet(company_file) |> select(gvkey, conm, sic)
#> # A tibble: 57,608 × 3
#>    gvkey  conm                     sic  
#>    <chr>  <chr>                    <chr>
#>  1 001000 A & E PLASTIK PAK INC    3089 
#>  2 001001 A & M FOOD SERVICES INC  5812 
#>  3 001002 AAI CORP                 3825 
#>  4 001003 A.A. IMPORTING CO INC    5712 
#>  5 001004 AAR CORP                 5080 
#>  6 001005 A.B.A. INDUSTRIES INC    3724 
#>  7 001006 ABC INDS INC             2711 
#>  8 001007 ABKCO INDUSTRIES INC     3652 
#>  9 001008 ABM COMPUTER SYSTEMS INC 3577 
#> 10 001009 ABS INDUSTRIES INC       3460 
#> # ℹ 57,598 more rows
```

### Identifying Table Names and Schemas

WRDS uses schema names such as `crsp`, `comp`, `audit`, and `bank`.

``` r

wrds_update_pq("funda", "comp", obs = 1000)
```

You can list tables in a WRDS schema from R:

``` r

crsp_tables <- wrds_get_tables("crsp", views = TRUE)
crsp_tables[1:10]
#>  [1] "acti"     "asia"     "asib"     "asic"     "asio"     "asix"    
#>  [7] "bmdebt"   "bmheader" "bmpaymts" "bmquotes"
```

Use the PostgreSQL schema name exposed by WRDS, which can differ from a
short product name or SAS library name. For example, the Fama-French
PostgreSQL base-table schema is `ff_all`, so a schema download starts
with:

``` r

wrds_get_tables("ff_all")
#>  [1] "factors_china"       "factors_daily"       "factors_monthly"    
#>  [4] "fivefactors_daily"   "fivefactors_monthly" "industry12"         
#>  [7] "industry48"          "liq_ps"              "liq_sadka"          
#> [10] "portfolios"          "portfolios25"        "portfolios_d"
```

However, these tables are exposed as **views** in the database schema
`ff`. Setting `views = TRUE` shows the underlying data made available
there.

``` r

wrds_schema_to_pq("ff", views = TRUE)
#> Processing 12 table(s) in schema 'ff'.
#> No comment found for ff.factors_china.
#> Beginning file download at 2026-05-26 13:55:39 UTC.
#> Completed file download at 2026-05-26 13:55:39 UTC.
#> Updated ff.factors_daily is available.
#> Beginning file download at 2026-05-26 13:55:40 UTC.
#> Completed file download at 2026-05-26 13:55:40 UTC.
#> Updated ff.factors_monthly is available.
#> Beginning file download at 2026-05-26 13:55:41 UTC.
#> Completed file download at 2026-05-26 13:55:41 UTC.
#> Updated ff.fivefactors_daily is available.
#> Beginning file download at 2026-05-26 13:55:42 UTC.
#> Completed file download at 2026-05-26 13:55:43 UTC.
#> No comment found for ff.fivefactors_monthly.
#> Beginning file download at 2026-05-26 13:55:43 UTC.
#> Completed file download at 2026-05-26 13:55:44 UTC.
#> Updated ff.industry12 is available.
#> Beginning file download at 2026-05-26 13:55:44 UTC.
#> Completed file download at 2026-05-26 13:55:45 UTC.
#> Updated ff.industry48 is available.
#> Beginning file download at 2026-05-26 13:55:45 UTC.
#> Completed file download at 2026-05-26 13:55:46 UTC.
#> Updated ff.liq_ps is available.
#> Beginning file download at 2026-05-26 13:55:47 UTC.
#> Completed file download at 2026-05-26 13:55:47 UTC.
#> Updated ff.liq_sadka is available.
#> Beginning file download at 2026-05-26 13:55:48 UTC.
#> Completed file download at 2026-05-26 13:55:48 UTC.
#> Updated ff.portfolios is available.
#> Beginning file download at 2026-05-26 13:55:49 UTC.
#> Completed file download at 2026-05-26 13:55:49 UTC.
#> Updated ff.portfolios25 is available.
#> Beginning file download at 2026-05-26 13:55:50 UTC.
#> Completed file download at 2026-05-26 13:55:51 UTC.
#> Updated ff.portfolios_d is available.
#> Beginning file download at 2026-05-26 13:55:51 UTC.
#> Completed file download at 2026-05-26 13:55:52 UTC.
```

The WRDS web query interface can also be useful for identifying the
underlying schema and table behind a dataset, but once the table is
known, you should download with a `db2pq` script for reproducibility.

## Cleaning and Shaping Data

### Correcting Data Types

Some WRDS columns are stored in PostgreSQL with a type that is not ideal
for analysis. For example, link-history identifiers in
`crsp.ccmxpf_lnkhist` are often better represented as integers:

``` r

wrds_update_pq(
  "ccmxpf_lnkhist", "crsp",
  col_types = list(lpermno = "int32", lpermco = "int32")
)
#> Updated crsp.ccmxpf_lnkhist is available.
#> Beginning file download at 2026-05-26 13:55:53 UTC.
#> Completed file download at 2026-05-26 13:55:54 UTC.
```

### Setting Time Zones

WRDS PostgreSQL often stores timestamps without time-zone information.
The `tz` argument tells `db2pq` how to interpret those values before
writing normalized timestamps to Parquet.

For example, in the FFIEC Call Report data in the WRDS `bank` schema,
timestamps should be interpreted as US Eastern time:

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

dsf_file = wrds_update_pq(
  "dsf",
  "crsp",
  obs = 100,
  force = TRUE,
  keep = c("permno", "date", "^ret$")
)
#> Forcing update based on user request.
#> Beginning file download at 2026-05-26 13:55:54 UTC.
#> Completed file download at 2026-05-26 13:55:55 UTC.

read_parquet(dsf_file)
#> # A tibble: 100 × 3
#>    permno date           ret
#>     <int> <date>       <dbl>
#>  1  10000 1986-01-07 NA     
#>  2  10000 1986-01-08 -0.0244
#>  3  10000 1986-01-09  0     
#>  4  10000 1986-01-10  0     
#>  5  10000 1986-01-13  0.05  
#>  6  10000 1986-01-14  0.0476
#>  7  10000 1986-01-15  0.0455
#>  8  10000 1986-01-16  0.0435
#>  9  10000 1986-01-17  0     
#> 10  10000 1986-01-20  0     
#> # ℹ 90 more rows
```

Note that the arguments to `keep` are evaluated as **regular
expressions**, so you want to use `"^ret$"` to match `ret` rather than
any column *containing* `ret`. For more on regular expressions, see
[Chapter 9](https://iangow.github.io/far_book/web-data.html) of
*Empirical Research in Accounting: Tools and Methods*.

### Renaming Variables

Use `rename` to write output columns with preferred names:

``` r

wrds_update_pq(
  "company",
  "comp",
  keep = c("gvkey", "conm", "sic"),
  alt_table_name = "sic_codes",
  rename = c(conm = "company_name")
)
```

For a small documentation example, the renamed file can be written to
the temporary repository:

``` r

renamed_company_file <- wrds_update_pq(
  "company",
  "comp",
  alt_table_name = "company_names",
  keep = c("gvkey", "conm", "sic"),
  rename = c(conm = "company_name"),
  force = TRUE
)
#> Forcing update based on user request.
#> Beginning file download at 2026-05-26 13:55:56 UTC.
#> Completed file download at 2026-05-26 13:55:56 UTC.

read_parquet(renamed_company_file) |>
  select(gvkey, company_name, sic)
#> # A tibble: 57,608 × 3
#>    gvkey  company_name             sic  
#>    <chr>  <chr>                    <chr>
#>  1 001000 A & E PLASTIK PAK INC    3089 
#>  2 001001 A & M FOOD SERVICES INC  5812 
#>  3 001002 AAI CORP                 3825 
#>  4 001003 A.A. IMPORTING CO INC    5712 
#>  5 001004 AAR CORP                 5080 
#>  6 001005 A.B.A. INDUSTRIES INC    3724 
#>  7 001006 ABC INDS INC             2711 
#>  8 001007 ABKCO INDUSTRIES INC     3652 
#>  9 001008 ABM COMPUTER SYSTEMS INC 3577 
#> 10 001009 ABS INDUSTRIES INC       3460 
#> # ℹ 57,598 more rows
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
local file while developing a refresh script.

``` r

funda_sample_file <- wrds_update_pq(
  "funda",
  "comp",
  alt_table_name = "funda_sample",
  keep = c("gvkey", "datadate", "fyear", "^at$", "^sale$"),
  where = paste(
    "indfmt = 'INDL'",
    "AND datafmt = 'STD'",
    "AND consol = 'C'",
    "AND popsrc = 'D'"
  ),
  obs = 1000,
  force = TRUE
)
#> Forcing update based on user request.
#> Beginning file download at 2026-05-26 13:55:57 UTC.
#> Completed file download at 2026-05-26 13:55:57 UTC.

read_parquet(funda_sample_file) |> glimpse()
#> Rows: 1,000
#> Columns: 5
#> $ gvkey    <chr> "001000", "001000", "001000", "001000", "001000", "001000", "…
#> $ datadate <date> 1961-12-31, 1962-12-31, 1963-12-31, 1964-12-31, 1965-12-31, …
#> $ fyear    <int> 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1…
#> $ at       <dbl> NA, NA, NA, 1.416, 2.310, 2.430, 2.456, 5.922, 28.712, 33.450…
#> $ sale     <dbl> 0.900, 1.600, 1.457, 2.032, 1.688, 4.032, 3.594, 7.400, 37.39…
```

## Update Logic

### Forcing an Update

Use `force = TRUE` when you want to re-download even if local metadata
suggest the file is current. This is useful after changing `keep`,
`drop`, `rename`, `col_types`, or `tz`.

``` r

wrds_update_pq("funda", "comp", force = TRUE, obs = 1000)
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

``` r

archived_company <- pq_archive("company", "comp")
#> Archived to: db2pq-wrds-to-parquet-examples/comp/archive/company_20260526T060000Z.parquet
basename(archived_company)
#> [1] "company_20260526T060000Z.parquet"

pq_last_modified("company", "comp", archive = TRUE) |>
  select(file_name, last_mod)
#> # A tibble: 1 × 2
#>   file_name                last_mod           
#>   <chr>                    <dttm>             
#> 1 company_20260526T060000Z 2026-05-26 06:00:00
```

The archived copy can be restored, again without touching the main
`DATA_DIR`.

``` r

archived_company_name <- basename(archived_company)

pq_restore(
  archived_company_name,
  "comp",
  archive = FALSE
)
#> Restored to: db2pq-wrds-to-parquet-examples/comp/company.parquet

pq_last_modified("company", "comp")
#> [1] "Company (Updated 2026-05-26)"
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

For a small query result, use `LIMIT`:

``` r

sql_query <- "
    SELECT gvkey, datadate, fyear, at, sale
    FROM comp.funda
    WHERE indfmt = 'INDL'
    AND datafmt = 'STD'
    AND consol = 'C'
    AND popsrc = 'D'
    ORDER BY gvkey, datadate
    LIMIT 20
  "

sql_file <- wrds_sql_to_pq(
  sql_query,
  table_name = "funda_sql_sample",
  schema = "comp",
  archive = TRUE
)

read_parquet(sql_file)
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
