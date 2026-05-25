# PostgreSQL to Parquet

This package provides some basic functionality for exporting data from a
PostgreSQL database to Parquet files. Because the existence of “last
updated” metadata cannot be assured with general PostgreSQL data
sources, I have *not* implemented the `pg_update_pq()` function
analogous to
[`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md).
Use the Python package for `pg_update_pq()`.

``` r

library(db2pq)
```

[`db_to_pq()`](https://iangow.github.io/db2pqr/reference/db_to_pq.md)
exports PostgreSQL tables to Parquet using a DBI connection. It can
filter rows, select columns, rename outputs, and apply Arrow type
overrides.

``` r

db_to_pq(
  table_name = "company",
  schema = "comp",
  data_dir = "~/pq_data",
  keep = c("gvkey", "conm"),
  rename = c(conm = "company_name"),
  col_types = list(company_name = "string")
)
```

Use
[`db_schema_tables()`](https://iangow.github.io/db2pqr/reference/db_schema_tables.md)
to inspect available relations and
[`db_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/db_schema_to_pq.md)
to export an entire schema.

``` r

db_schema_tables("ff")
#>  [1] "factors_china"       "factors_daily"       "factors_monthly"    
#>  [4] "fivefactors_daily"   "fivefactors_monthly" "industry12"         
#>  [7] "industry48"          "liq_ps"              "liq_sadka"          
#> [10] "portfolios"          "portfolios25"        "portfolios_d"
```

``` r

db_schema_to_pq("ff", data_dir = "~/pq_data")
```

## PostgreSQL Numeric Columns

With the default `numeric_mode = "decimal"`, table exports preserve
bounded PostgreSQL `NUMERIC(p, s)` columns as Parquet decimal columns.

``` r

db_to_pq(
  table_name = "dsi",
  schema = "crsp",
  data_dir = "~/pq_data"
)
```

``` r

library(arrow)
#> 
#> Attaching package: 'arrow'
#> The following object is masked from 'package:utils':
#> 
#>     timestamp

read_parquet("~/pq_data/crsp/dsi.parquet", as_data_frame = FALSE)
#> Table
#> 26051 rows x 11 columns
#> $date <date32[day]>
#> $vwretd <decimal32(9, 6)>
#> $vwretx <decimal32(9, 6)>
#> $ewretd <decimal32(9, 6)>
#> $ewretx <decimal32(9, 6)>
#> $sprtrn <decimal32(9, 6)>
#> $spindx <decimal32(7, 2)>
#> $totval <decimal64(13, 2)>
#> $totcnt <int32>
#> $usdval <decimal64(13, 2)>
#> $usdcnt <int32>
#> 
#> See $metadata for additional Schema metadata
```

Any PostgreSQL `NUMERIC` columns without precision and scale remain text
because a Parquet decimal type requires those bounds.

Use `numeric_mode = "text"` to retain exact text values for all detected
`NUMERIC` columns, `numeric_mode = "float64"` when double-precision
output is the intended representation, or `numeric_mode = "raw"` to keep
the backend default.

``` r

db_to_pq(
  table_name = "dsi",
  schema = "crsp",
  data_dir = "~/pq_data",
  numeric_mode = "float64"
)
```

``` r

read_parquet("~/pq_data/crsp/dsi.parquet", as_data_frame = FALSE)
#> Table
#> 26051 rows x 11 columns
#> $date <date32[day]>
#> $vwretd <double>
#> $vwretx <double>
#> $ewretd <double>
#> $ewretx <double>
#> $sprtrn <double>
#> $spindx <double>
#> $totval <double>
#> $totcnt <int32>
#> $usdval <double>
#> $usdcnt <int32>
#> 
#> See $metadata for additional Schema metadata
```

While the R package does not implement general “update logic” of the
WRDS-facing functions, table comments are retained as Parquet metadata.

``` r

pq_last_modified("dsi", "crsp", data_dir = "~/pq_data")
#> [1] "Stock - Market Indexes Daily NYSE/AMEX/NASDAQ/ARCA (Updated 2025-02-08)"
```

## Database connections

The
[`db_to_pq()`](https://iangow.github.io/db2pqr/reference/db_to_pq.md)
function accepts arguments for `host`, `database`, `user`, `password`,
and `port`. The default values for these are taken from environment
variables `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and `PGPORT`,
respectively. Values for these arguments can also be given to
[`db_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/db_schema_to_pq.md),
which will pass them along to the
[`db_to_pq()`](https://iangow.github.io/db2pqr/reference/db_to_pq.md)
calls it makes.
