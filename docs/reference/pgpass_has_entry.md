# Check for a matching `.pgpass` entry

Check for a matching `.pgpass` entry

## Usage

``` r
pgpass_has_entry(host, port = 5432L, database, user, passfile = pgpass_path())
```

## Arguments

- host, port, database, user:

  PostgreSQL connection target.

- passfile:

  Path to the password file.

## Value

`TRUE` if a matching entry exists, otherwise `FALSE`.
