# Authentication

`db2pq` separates two pieces of WRDS PostgreSQL authentication:

1.  identifying the WRDS username
2.  letting PostgreSQL find a password for that username

The goal is to keep WRDS refresh scripts free of hard-coded credentials
while still working with credential setups that are already common in R.

## The Short Version

For a reproducible WRDS setup:

1.  Put your WRDS username in `WRDS_ID` or `WRDS_USER`.
2.  Put the WRDS PostgreSQL password in PostgreSQL’s `.pgpass` file.
3.  Use
    [`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
    to test the connection before a long export.

``` r

library(db2pq)

wrds_get_username()
wrds_check_credentials()
```

The `wrds` package’s keyring setup remains supported for existing R
workflows:

``` r

wrds::wrds_set_credentials()
```

## Username Resolution

For credential helpers, `db2pq` resolves a WRDS username in this order:

- an explicit `wrds_id` argument
- `WRDS_ID`
- `WRDS_USER`
- the keyring username created by
  [`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html)

`WRDS_USER` is accepted mainly for compatibility with Tidy Finance-style
setups.

``` r

Sys.setenv(WRDS_ID = "your_wrds_id")

wrds_get_username()
wrds_conninfo(format = "uri")
```

An explicit function argument can override environment or keyring
defaults:

``` r

wrds_update_pq("company", "comp", wrds_id = "your_wrds_id")
```

## Environment Variables in R

[`Sys.setenv()`](https://rdrr.io/r/base/Sys.setenv.html) is useful for a
running R session:

``` r

Sys.setenv(
  WRDS_ID = "your_wrds_id",
  DATA_DIR = "~/Dropbox/pq_data"
)
```

For settings you want available when R starts, use `.Renviron`. This is
the closest R analogue to a simple environment-variable file for this
workflow. R searches for `.Renviron` in the current working directory
and then in the user’s home directory, so there are two useful patterns:

- `~/.Renviron`: user-level defaults that apply broadly on your machine
- `.Renviron` in a project directory: project-local values for that
  project

A user-level `~/.Renviron` is a good place for a default WRDS username
or default Parquet repository:

``` text
WRDS_ID=your_wrds_id
DATA_DIR=~/Dropbox/pq_data
```

A project-local `.Renviron` is useful when one project should override
the user-level defaults without changing other R work on the same
machine. For example, it can select a project-specific Parquet
repository and, when needed, a project-specific WRDS username:

``` text
WRDS_ID=project_wrds_id
DATA_DIR=~/research/project-data/pq_data
```

The environment file should not be committed if it contains private,
machine-specific, or secret values. After editing a `.Renviron` file,
start a new R session or load it explicitly with
[`readRenviron()`](https://rdrr.io/r/base/readRenviron.html).

## Password Handling

WRDS helpers eventually connect to PostgreSQL. For the password, prefer
PostgreSQL’s native password-file mechanism, `.pgpass`, rather than
putting a password in R code.

The default password-file path used by
[`pgpass_path()`](https://iangow.github.io/db2pqr/reference/pgpass_path.md)
is:

- `~/.pgpass` on macOS and Linux
- `%APPDATA%/postgresql/pgpass.conf` on Windows

``` r

pgpass_path()
```

A WRDS `.pgpass` entry has the PostgreSQL password-file shape:

``` text
wrds-pgdata.wharton.upenn.edu:9737:wrds:your_wrds_id:your_wrds_password
```

On macOS and Linux, PostgreSQL expects the file to be readable only by
the owner:

``` sh
chmod 600 ~/.pgpass
```

This setup also works with other libpq-aware PostgreSQL tools, not only
`db2pq`.

## Inspect and Test the Setup

[`pgpass_has_entry()`](https://iangow.github.io/db2pqr/reference/pgpass_has_entry.md)
checks whether the default password file has a matching WRDS entry
without printing the password:

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
for a live connection test:

``` r

wrds_check_credentials()
```

The returned diagnostic reports whether a small WRDS PostgreSQL
connection succeeded and whether the matching target appears in
`.pgpass`.

## Existing `wrds` Package Credentials

If you already use the `wrds` package, its keyring-based setup is still
a valid path:

``` r

wrds::wrds_set_credentials()
wrds_update_pq("company", "comp")
```

Use this route when you want to stay close to existing R WRDS code. Use
the environment-variable plus `.pgpass` route when you want credentials
to work across `db2pq`, RPostgres/libpq, and other PostgreSQL tools on
the same machine.

## SAS-Based Workflows

Most `db2pq` WRDS work uses WRDS PostgreSQL. If you set
`use_sas = TRUE`, `db2pq` also retrieves metadata from the WRDS SAS side
over SSH. That SSH setup is separate from PostgreSQL password handling;
see the [WRDS SSH setup
article](https://iangow.github.io/db2pqr/articles/wrds-ssh.md) before
using SAS metadata in unattended refresh scripts.

## Practical Recommendation

- Put a default `WRDS_ID` and `DATA_DIR` in user-level `~/.Renviron` if
  they apply across projects.
- Use a project-local `.Renviron` when a project should override
  `WRDS_ID` or `DATA_DIR`.
- Store the WRDS PostgreSQL password in `.pgpass`.
- Keep
  [`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html)
  as a compatibility route for existing R WRDS workflows.

## Related Pages

- WRDS to Parquet article
- Data management article
- [`wrds_get_username()`](https://iangow.github.io/db2pqr/reference/wrds_get_username.md)
- [`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
- [`pgpass_path()`](https://iangow.github.io/db2pqr/reference/pgpass_path.md)
