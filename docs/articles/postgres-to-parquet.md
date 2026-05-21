# PostgreSQL to Parquet

[`db_to_pq()`](https://iandgow.github.io/db2pqr/reference/db_to_pq.md)
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
[`db_schema_tables()`](https://iandgow.github.io/db2pqr/reference/db_schema_tables.md)
to inspect available relations and
[`db_schema_to_pq()`](https://iandgow.github.io/db2pqr/reference/db_schema_to_pq.md)
to export a schema.
