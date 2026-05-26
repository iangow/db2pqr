# db2pq: export PostgreSQL and WRDS data to Parquet


I created `db2pq` as an R package for moving data from PostgreSQL into
Apache Parquet files. I believe that Parquet files are the best file
format for researchers and data analysts who want “local” data, but
without the hassle of managing a relational database server. The design
of the `db2pq` R package is modelled on that of [my Python
package](https://pypi.org/project/db2pq/) of the same name. However, I
have elected to keep the R package focused on a PostgreSQL-to-Parquet
pathway. If you want to get Parquet data into a PostgreSQL database or
move data from one PostgreSQL database to another, then look at [the
Python package](https://pypi.org/project/db2pq/) for those pathways.

This package is designed both for general PostgreSQL sources and for the
WRDS PostgreSQL service.

## What does `db2pq` do?

Some examples of what `db2pq` does are:

- Exports a single PostgreSQL table to Parquet.
- Exports all tables in a PostgreSQL schema to Parquet.
- Updates WRDS Parquet files only when the source table is newer.
- Reads and manages `last_modified` metadata embedded in Parquet files.
- Archives and restores different versions of Parquet files.

## Installation

To install from CRAN:

``` r
install.packages("db2pq")
```

Or use `pak` to get the version from GitHub:

``` r
# install.packages("pak")
pak::pak("iangow/db2pqr")
```

## Quickstart

### Load the package

Once you have installed `db2pq`, you can load into R it with
`library()`.

``` r
library(db2pq)
```

### Update a WRDS table

The main WRDS-related function is `wrds_update_pq()`. By default, it
downloads a table only when WRDS metadata indicate that the source is
newer than the local Parquet file.

``` r
wrds_update_pq("dsi", "crsp")
```

    crsp.dsi already up to date.

If you have not already configured your WRDS username and password using
one of the mechanisms described
[here](https://iangow.github.io/db2pqr/articles/authentication.html),
then an interactive first call prompts for them. After a successful
connection, `db2pq` can reuse the saved username and `.pgpass` password
entry in later sessions.

If `DATA_DIR` has not been set, an interactive first call also helps
choose the local Parquet repository root. The [DATA_DIR
article](https://iangow.github.io/db2pqr/articles/data-dir.html)
explains when to use a shared setting, a project-specific setting, or an
explicit `data_dir` argument.

### Force a re-download

If you want to overwrite existing data, you can use `force = TRUE`.

``` r
wrds_update_pq("dsi", "crsp", force = TRUE)
```

    Forcing update based on user request.

    Beginning file download at 2026-05-23 16:09:10 UTC.

    Completed file download at 2026-05-23 16:09:11 UTC.

### Using SAS metadata for updates

The update logic of `wrds_update_pq()` works by comparing information
stored by WRDS in the PostgreSQL table comment with that retrieved and
stored from previous runs of `wrds_update_pq()`. However, in some cases
the PostgreSQL table comments are missing or may not provide the signal
of “freshness” that we want to use.

WRDS data tables generally come in two forms: PostgreSQL tables and SAS
files. The `wrds_update_pq()` function allows a user to specify
`use_sas = TRUE`, which causes the function to check the “last updated”
metadata from the corresponding WRDS SAS file instead.

In some cases, the schema used for SAS differs from that used for
PostgreSQL and `wrds_update_pq()` can handle such cases using the
`sas_schema` argument. For example, the WRDS PostgreSQL schema and the
SAS schema do not line up for RavenPack and the following code handles
this.

``` r
wrds_update_pq("rpa_entity_mappings", "ravenpack_common",
               use_sas = TRUE, sas_schema = "rpa")
```

    ravenpack_common.rpa_entity_mappings already up to date.

Additional setup is needed for this SAS metadata path; see the [WRDS SSH
setup article](https://iangow.github.io/db2pqr/articles/wrds-ssh.html)
for details.

### Update all tables in a schema

``` r
wrds_schema_to_pq("ff", views = TRUE)
```

    Processing 12 table(s) in schema 'ff'.

    No comment found for ff.factors_china.
    Use `force = TRUE` to update without relying on metadata, or
    `use_sas = TRUE` to use SAS metadata.
    ff.factors_daily already up to date.
    ff.factors_monthly already up to date.
    ff.fivefactors_daily already up to date.
    No comment found for ff.fivefactors_monthly.
    Use `force = TRUE` to update without relying on metadata, or
    `use_sas = TRUE` to use SAS metadata.
    ff.industry12 already up to date.
    ff.industry48 already up to date.
    ff.liq_ps already up to date.
    ff.liq_sadka already up to date.
    ff.portfolios already up to date.
    ff.portfolios25 already up to date.
    ff.portfolios_d already up to date.

### Export a custom WRDS SQL query

``` r
wrds_sql_to_pq("
  SELECT permno, date, ret 
  FROM crsp.dsf 
  WHERE date >= '2024-01-01'",
  table_name = "dsf_recent",
  schema = "my_project"
)
```

### Export a local PostgreSQL table

``` r
db_to_pq(
  table_name = "company",
  schema = "comp",
  keep = c("gvkey", "conm"),
  rename = c(conm = "company_name")
)
```

### Check when local Parquet files were last updated

``` r
pq_last_modified(schema = "crsp")
```

    # A tibble: 33 × 5
       file_name        table            schema last_mod            last_mod_str    
       <chr>            <chr>            <chr>  <dttm>              <chr>           
     1 ccmxpf_linktable ccmxpf_linktable crsp   2026-02-06 07:00:00 CRSP/COMPUSTAT …
     2 ccmxpf_lnkhist   ccmxpf_lnkhist   crsp   2026-02-06 07:00:00 Native Link usa…
     3 ccmxpf_lnkused   ccmxpf_lnkused   crsp   2026-02-06 07:00:00 LINKUSED struct…
     4 comphist         comphist         crsp   2026-02-06 07:00:00 CRSP/COMPUSTAT …
     5 dse              dse              crsp   2025-02-08 07:00:00 Daily Stock - E…
     6 dsedelist        dsedelist        crsp   2025-02-08 07:00:00 CRSP Daily Stoc…
     7 dsedist          dsedist          crsp   2025-02-08 07:00:00 CRSP Daily Stoc…
     8 dseexchdates     dseexchdates     crsp   2025-01-18 23:37:28 Last modified: …
     9 dsf              dsf              crsp   2025-02-08 07:00:00 Daily Stock - S…
    10 dsf_v2           dsf_v2           crsp   2026-02-06 07:00:00 Daily Stock Fil…
    # ℹ 23 more rows

## Parquet layout

Files are organized as:

    <DATA_DIR>/<schema>/<table>.parquet

For example:

    ~/pq_data/crsp/dsi.parquet

The `DATA_DIR` environment variable sets the root directory. It can also
be passed directly as `data_dir` to any function. For first-time setup,
an interactive Parquet helper can select or create the directory and save
`DATA_DIR` in project-level or user-level `.Renviron`. Use `pq_data_dir()`
to inspect or resolve the directory that `db2pq` will use by default.

When `archive = TRUE`, replaced files are moved to:

    <DATA_DIR>/<schema>/archive/<table>_<timestamp>.parquet

## License

MIT License. See `LICENSE.md`.
