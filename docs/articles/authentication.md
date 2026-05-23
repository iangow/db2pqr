# Authentication

There are two elements required for a WRDS PostgreSQL authentication:

1.  the WRDS username
2.  the WRDS password for that username

One goal of the `db2pq` package is to keep scripts used to manage WRDS
data free of hard-coded credentials. In addition, the `db2pq` package
endeavours to build on existing credential management processes that
users may have.

## Resolving the WRDS username

For credential helpers, `db2pq` resolves a WRDS username using the
following order:

1.  an explicit `wrds_id` argument supplied to the function
2.  the `WRDS_ID` environment variable
3.  the `WRDS_USER` environment variable[^1]
4.  the keyring username created by
    [`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html)

Because of this order, an explicit function argument can be used to
override environment variables or keyring defaults:

``` r

wrds_update_pq("company", "comp", wrds_id = "your_wrds_id")
```

The
[`wrds_get_username()`](https://iangow.github.io/db2pqr/reference/wrds_get_username.md)
function can be used to see the resolved WRDS username:

``` r

wrds_get_username()
```

If
[`wrds_get_username()`](https://iangow.github.io/db2pqr/reference/wrds_get_username.md)
does not resolve to a WRDS username using the process described above,
then the user is prompted to supply one. The supplied username is used
for the current session and the user is given the option to save the
username for future R sessions.

## Resolving the WRDS password

Once `db2pq` has resolved a WRDS username, it needs a password for that
username to connect to the WRDS PostgreSQL database.

Passwords are resolved in the following order:

1.  an explicit `password` argument, where the function supports one
2.  a matching entry in PostgreSQL’s `.pgpass` password file
3.  an existing `WRDS_PASSWORD` environment variable[^2]
4.  an interactive password prompt when no matching `.pgpass` entry or
    `WRDS_PASSWORD` is found[^3]

If the username can only be resolved from credentials created by
[`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html),
`db2pq` falls back to
[`wrds::wrds_connect()`](https://rdrr.io/pkg/wrds/man/wrds_connect.html).
That route lets the `wrds` package use its keyring credentials.

The
[`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
function can be used to test the resolved WRDS username and password:

``` r

wrds_check_credentials()
```

If an interactive `db2pq` connection prompts for a password and the
connection succeeds, the user-supplied password is saved to `.pgpass`
for future connections. When `db2pq` uses an existing `WRDS_PASSWORD`
because `.pgpass` has no matching entry, it saves that password to
`.pgpass` after a successful connection too.

An explicit password can also be checked without saving it:

``` r

wrds_check_credentials(password = "your_wrds_password", save = FALSE)
```

PostgreSQL’s `.pgpass` mechanism is preferred to putting a password in R
code. It also works with RPostgres/libpq and other PostgreSQL tools, not
only `db2pq`.

The default password-file path used by
[`pgpass_path()`](https://iangow.github.io/db2pqr/reference/pgpass_path.md)
is:

- `~/.pgpass` on macOS and Linux
- `%APPDATA%/postgresql/pgpass.conf` on Windows

A WRDS `.pgpass` entry has the PostgreSQL password-file shape:

``` text
wrds-pgdata.wharton.upenn.edu:9737:wrds:your_wrds_id:your_wrds_password
```

On macOS and Linux, PostgreSQL expects the file to be readable only by
the owner. Password entries saved by `db2pq` use that permission
automatically; for a file created manually, set it with:

``` sh
chmod 600 ~/.pgpass
```

## Inspect and Test the Setup

Use
[`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
for a live connection test:

``` r

wrds_check_credentials()
```

If the matching WRDS entry is not already in `.pgpass`, an interactive
[`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
call asks for the WRDS PostgreSQL password. When the connection test
succeeds, it writes that password to `.pgpass` for later libpq-backed
connections. You can pass `password = ...` explicitly for a
non-interactive setup check, use `save = FALSE` for a one-time check, or
use `passfile = ...` to target a specific PostgreSQL password file.

The returned diagnostic reports whether a small WRDS PostgreSQL
connection succeeded and whether the matching target appears in
`.pgpass`.

## Existing `wrds` Package Credentials

If you already use the `wrds` package, its keyring-based setup remains a
valid path:

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
repository:

``` text
DATA_DIR=~/research/project-data/pq_data
```

The environment file should not be committed if it contains private,
machine-specific, or secret values. After editing a `.Renviron` file,
start a new R session or load it explicitly with
[`readRenviron()`](https://rdrr.io/r/base/readRenviron.html).

For first-time `DATA_DIR` setup, call
[`db2pq_data_dir()`](https://iangow.github.io/db2pqr/reference/db2pq_data_dir.md)
or let a Parquet helper call it as its default. In an interactive
session without a `DATA_DIR`, the helper asks for a data directory, can
create a new directory if needed, and offers to store `DATA_DIR` in
either project-level `.Renviron` or user-level `~/.Renviron`. That scope
choice is useful for data repositories; the interactive WRDS username
prompt stores `WRDS_ID` in user-level `~/.Renviron`.

## Related Pages

- WRDS to Parquet article
- Data management article
- [`wrds_get_username()`](https://iangow.github.io/db2pqr/reference/wrds_get_username.md)
- [`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
- [`pgpass_path()`](https://iangow.github.io/db2pqr/reference/pgpass_path.md)

[^1]: `WRDS_USER` is accepted mainly for compatibility with the Tidy
    Finance setup.

[^2]: `WRDS_PASSWORD` is read for compatibility with the Tidy Finance
    setup. `db2pq` does not create or persist this environment variable.

[^3]: When prompting is disabled and `db2pq` passes no password to the
    connection, PostgreSQL/libpq may still use another credential
    mechanism, such as `PGPASSWORD`.
