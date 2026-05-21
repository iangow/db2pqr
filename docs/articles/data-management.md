# Data Management for Research

This article adapts a broader set of data-management ideas for the
narrower task that `db2pq` is designed to support: building and
maintaining a local Parquet repository from WRDS or another PostgreSQL
database.

The goal is to separate three concerns that often get mixed together in
research code:

1.  Getting data from the source system.
2.  Storing a reusable local copy in an efficient format.
3.  Running analysis code against that local copy.

`db2pq` focuses on the first two steps. Analysis code can then use
packages such as `arrow`, `duckdb`, `dplyr`, or `polars` to work with
the Parquet files.

## Principles

Good research data management is mostly about making later work boring:
scripts should be rerunnable, file locations should be predictable, and
it should be clear which files came from an external source and which
files were created by project code.

Several principles are useful:

1.  Keep source data immutable where practical.
2.  Make derived data reproducible from code.
3.  Use stable, documented file locations.
4.  Record enough metadata to know when a file was created and what
    source it represents.
5.  Avoid putting large, private, or licensed data files in Git.

`db2pq` is not a full project-management framework. It supplies a narrow
piece of infrastructure: a repeatable way to create local Parquet copies
of database tables and to inspect the metadata attached to those copies.

## Prefer Portable File Formats

Research data often outlives the first tool used to analyze it. Parquet
is a useful storage format because it is columnar, compressed, typed,
and readable from many systems. A local Parquet repository can be
queried from R, Python, DuckDB, Arrow, Spark, and other tools without
repeatedly connecting to WRDS.

This is especially useful when a project uses more than one language. A
refresh script can use `db2pq` to create files, while later analysis can
read the same files from R or Python.

## Use a Stable Data Directory

Set `DATA_DIR` once per project or machine so that scripts can refer to
a logical data repository without hard-coding paths throughout the
analysis.

``` r

Sys.setenv(DATA_DIR = "~/Dropbox/pq_data")
```

Files created by `db2pq` are stored using the layout:

``` text
<DATA_DIR>/<schema>/<table>.parquet
```

For example:

``` text
~/Dropbox/pq_data/crsp/dsi.parquet
~/Dropbox/pq_data/comp/company.parquet
```

## Treat Downloads as a Refresh Step

The scripts that download data should usually be separate from the
scripts that analyze data. A refresh script might contain calls like
these:

``` r

library(db2pq)

wrds_update_pq("dsi", "crsp")
wrds_update_pq("company", "comp")
wrds_update_pq("funda", "comp", keep = c("gvkey", "datadate", "at", "sale"))
```

Because
[`wrds_update_pq()`](https://iandgow.github.io/db2pqr/reference/wrds_update_pq.md)
compares WRDS metadata with metadata embedded in the local Parquet file,
it can be rerun without downloading a table that is already current.

The same idea applies to non-WRDS PostgreSQL sources. Put source-system
refresh logic in one place, then let analysis code consume the resulting
files.

## Keep Raw and Derived Data Separate

A useful convention is to keep source-table exports in one repository
and write derived research datasets somewhere else. For example:

``` text
~/Dropbox/pq_data/          # Local source-table repository
data/derived/               # Project-specific derived files
```

The source-table repository should be reproducible from WRDS or
PostgreSQL. Derived files should be reproducible from project code.

This separation matters because source data and derived data have
different rules. Source exports should be refreshed deliberately and
should preserve the database table as closely as is useful. Derived
files can be redesigned as the research design changes.

## Make Transformations Explicit

Avoid hiding important filters, joins, and sample restrictions inside ad
hoc interactive work. Put them in scripts or functions. A useful pattern
is:

``` text
source database -> local Parquet repository -> derived research data -> tables/figures
```

`db2pq` can help with the first arrow. The later arrows should usually
live in project-specific code, where sample construction choices are
visible to readers and coauthors.

## Record Metadata

Parquet files written by WRDS helpers include `last_modified` metadata
when the source table provides it. Use
[`pq_last_modified()`](https://iandgow.github.io/db2pqr/reference/pq_last_modified.md)
to audit a repository:

``` r

pq_last_modified(schema = "crsp")
pq_last_modified(schema = "comp")
```

For data-management work, this is often more useful than relying only on
local file modification times, because the metadata records what `db2pq`
knows about the source table.

Metadata will not answer every provenance question, but it gives a
lightweight audit trail. For more complex projects, combine it with
project logs, scripts, or a data dictionary that records table sources
and refresh dates.

## Archive Before Replacing

For tables where changes matter, use `archive = TRUE` so an existing
file is moved aside before it is replaced:

``` r

wrds_update_pq("company", "comp", archive = TRUE)
pq_last_modified("company", "comp", archive = TRUE)
```

Archives are stored under:

``` text
<DATA_DIR>/<schema>/archive/
```

This gives you a simple local history without turning the data
repository into a Git repository.

Archiving is most useful for relatively small reference tables or for
cases where source revisions may affect published results. For very
large tables, keeping every historical copy may be too expensive; in
those cases, record the refresh date and source metadata instead.

## Keep Large Data Out of Git

Code, documentation, and small test fixtures belong in Git. Full WRDS
exports generally do not. If a package or project needs example data,
use a small, non-sensitive file and place it deliberately in
`inst/extdata` or another documented location.

For package documentation, build examples so they can run without
private WRDS credentials. Longer examples can be shown with
`eval = FALSE`, while the local rendered website can still refer to
small local Parquet fixtures when those are safe to publish.

## Plan for Collaboration

Shared data workflows should minimize assumptions about one person’s
machine. Use environment variables, relative project paths, and
documented setup steps instead of absolute paths such as
`/Users/name/...`.

For WRDS credentials, prefer setup that each user can reproduce locally:

``` r

Sys.setenv(WRDS_ID = "your_wrds_id")
wrds_check_credentials()
```

Do not store credentials in project scripts. The package helpers are
designed to work with environment variables, `.pgpass`, and the
credential mechanisms used by the `wrds` package.

## Suggested Project Pattern

A compact project structure might look like this:

``` text
analysis-project/
  R/
  scripts/
    refresh-data.R
  data/
    derived/
  reports/
  renv.lock
```

The refresh script owns calls to `db2pq`. Analysis scripts read Parquet
files from `DATA_DIR` and write project-specific outputs to
`data/derived`.
