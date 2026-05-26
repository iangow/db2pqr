# ADBC backend

The stable default transfer path uses DBI/RPostgres. The optional
*experimental* ADBC path can stream Arrow batches directly from
PostgreSQL, but it depends on local driver builds and SSL support, which
the CRAN version of `adbcpostgresql` does not provide.

``` r

library(db2pq)
```

``` r

adbc_diagnostics()
wrds_update_pq("dsi", "crsp", transfer_method = "adbc")
```

If the ADBC path reports an SSL/libpq error, use
`transfer_method = "dbi"` or use an ADBC PostgreSQL driver that is built
against an SSL-capable `libpq`. WRDS requires SSL, and the stable WRDS
path remains DBI/RPostgres.
