# List PostgreSQL relations in a schema

List PostgreSQL relations in a schema

## Usage

``` r
db_schema_tables(
  schema,
  views = FALSE,
  con = NULL,
  host = Sys.getenv("PGHOST", "localhost"),
  database = Sys.getenv("PGDATABASE", unset = Sys.getenv("PGUSER")),
  user = Sys.getenv("PGUSER", Sys.info()[["user"]]),
  password = Sys.getenv("PGPASSWORD", ""),
  port = as.integer(Sys.getenv("PGPORT", 5432))
)
```

## Arguments

- schema:

  PostgreSQL schema name.

- views:

  If `TRUE`, include views as well as base tables.

- con:

  Optional existing DBI connection.

- host, database, user, password, port:

  PostgreSQL connection parameters used when `con` is not supplied.

## Value

A character vector of relation names.

## Examples

``` r
if (FALSE) { # \dontrun{
# Requires a PostgreSQL connection with the target schema
con <- DBI::dbConnect(RPostgres::Postgres())
db_schema_tables("crsp", con = con)
DBI::dbDisconnect(con)
} # }
```
