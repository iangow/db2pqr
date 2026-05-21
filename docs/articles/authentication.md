# Authentication

`db2pq` resolves WRDS usernames from an explicit `wrds_id`, `WRDS_ID`,
`WRDS_USER`, and finally the keyring entry created by
[`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html).

``` r

Sys.setenv(WRDS_ID = "your_wrds_id")
wrds_get_username()
wrds_conninfo(format = "uri")
```

For reproducible PostgreSQL access, prefer libpq’s `.pgpass` file where
possible. You can inspect whether a matching WRDS entry exists without
opening a database connection:

``` r

pgpass_has_entry(
  host = "wrds-pgdata.wharton.upenn.edu",
  port = 9737,
  database = "wrds",
  user = wrds_get_username()
)
```

Use
[`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
to test a live connection when WRDS access is available.
