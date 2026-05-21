# WRDS to Parquet

[`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)
exports a WRDS PostgreSQL table to a local Parquet file and skips the
download when existing metadata indicate that the local file is current.

``` r

Sys.setenv(DATA_DIR = "~/pq_data")

wrds_update_pq("dsi", "crsp")
wrds_update_pq("dsf", "crsp", obs = 1000, keep = c("permno", "date", "ret"))
wrds_update_pq("company", "comp", rename = c(conm = "company_name"))
```

Use
[`wrds_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_schema_to_pq.md)
for schema-wide refreshes and
[`wrds_sql_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_sql_to_pq.md)
for custom WRDS SQL queries.
