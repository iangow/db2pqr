# ADBC Backend

The stable default transfer path uses DBI/RPostgres. The optional ADBC
path can stream Arrow batches directly from PostgreSQL, but it depends
on local driver builds and SSL support.

As of April 2026, Apache Arrow ADBC 23 is current and the R
`adbcpostgresql` package has a 0.23.0 CRAN release. Use
[`adbc_diagnostics()`](https://iandgow.github.io/db2pqr/reference/adbc_diagnostics.md)
to inspect the optional packages installed in your R library:

``` r

adbc_diagnostics()
wrds_update_pq("dsi", "crsp", transfer_method = "adbc")
```

If the ADBC path reports an SSL/libpq error, use
`transfer_method = "dbi"` or install a current SSL-capable
`adbcpostgresql` build.
