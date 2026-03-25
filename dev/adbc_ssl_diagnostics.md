# ADBC PostgreSQL SSL Diagnostics

Use this note to compare the working and non-working machines when
`adbcpostgresql` fails with:

```text
IO: [libpq] Failed to connect: sslmode value "require" invalid when SSL support is not compiled in
```

The goal is to determine whether the two machines differ in:

- installed `adbcpostgresql` package build
- linked or embedded PostgreSQL client runtime
- local PostgreSQL client toolchain
- architecture / R build / package source

## 1. R-side package diagnostics

Run this in R on both machines:

```r
cat("== adbcpostgresql package info ==\n")
print(packageVersion("adbcpostgresql"))
print(find.package("adbcpostgresql"))
print(system.file("libs", package = "adbcpostgresql"))
print(packageDescription("adbcpostgresql")[c("Package", "Version", "Built", "Repository", "RemoteType", "RemoteSha")])

cat("\n== sessionInfo() ==\n")
print(sessionInfo())
```

What to compare:

- `Version`
- `Built`
- `Repository`
- `RemoteSha`
- R version
- platform / architecture

## 2. Direct ADBC SSL connection test

Run this in R on both machines. Replace `YOUR_WRDS_USER` and set `PGPASSWORD`
first if needed.

```r
library(DBI)
library(adbi)
library(adbcpostgresql)

con <- dbConnect(
  adbi::adbi("adbcpostgresql"),
  uri = "postgresql://YOUR_WRDS_USER@wrds-pgdata.wharton.upenn.edu:9737/wrds?sslmode=require"
)

dbGetQuery(con, "SELECT 1 AS ok")
dbDisconnect(con)
```

Interpretation:

- If this succeeds, the local ADBC PostgreSQL stack supports SSL.
- If this fails with the same `libpq` error, the issue is outside `db2pq`.

## 3. Shell-side system diagnostics

Run this in the shell on both machines:

```sh
echo "== basic system info =="
uname -a
which R
R --version

echo
echo "== postgres client tools =="
which pg_config
pg_config --version
pg_config --configure

echo
echo "== adbcpostgresql shared library path =="
Rscript -e 'cat(file.path(system.file("libs", package="adbcpostgresql"), "adbcpostgresql.so"), "\n")'

echo
echo "== dynamic linkage for adbcpostgresql =="
otool -L "$(Rscript -e 'cat(file.path(system.file("libs", package="adbcpostgresql"), "adbcpostgresql.so"))')"
```

What to compare:

- `pg_config` location
- PostgreSQL client version
- PostgreSQL configure flags
- whether the installed `adbcpostgresql.so` differs across machines

## 4. Optional environment check

If one machine was built from source or has non-default library paths, run:

```sh
echo "== relevant environment variables =="
env | grep -E '^(PATH|PKG_CONFIG_PATH|DYLD_LIBRARY_PATH|LIBRARY_PATH|CPATH|LDFLAGS|CPPFLAGS)='
```

This helps identify cases where one machine is picking up Homebrew or
Postgres.app client libraries and the other is not.

## 5. Single copy-paste shell collector

If you want one shell command that prints most of the useful information at
once, use this:

```sh
echo "== basic system info =="
uname -a
which R
R --version
echo
echo "== postgres client tools =="
which pg_config
pg_config --version
pg_config --configure
echo
echo "== relevant environment variables =="
env | grep -E '^(PATH|PKG_CONFIG_PATH|DYLD_LIBRARY_PATH|LIBRARY_PATH|CPATH|LDFLAGS|CPPFLAGS)='
echo
echo "== adbcpostgresql shared library path =="
Rscript -e 'cat(file.path(system.file("libs", package="adbcpostgresql"), "adbcpostgresql.so"), "\n")'
echo
echo "== adbcpostgresql dynamic linkage =="
otool -L "$(Rscript -e 'cat(file.path(system.file("libs", package="adbcpostgresql"), "adbcpostgresql.so"))')"
echo
echo "== adbcpostgresql package metadata =="
Rscript -e 'print(packageVersion("adbcpostgresql")); print(find.package("adbcpostgresql")); print(system.file("libs", package = "adbcpostgresql")); print(packageDescription("adbcpostgresql")[c("Package", "Version", "Built", "Repository", "RemoteType", "RemoteSha")]); print(sessionInfo())'
```

## 6. If the working machine differs

Likely explanations:

- different architecture-specific binary
- different R build
- different `adbcpostgresql` build origin
- different PostgreSQL client runtime bundled or resolved by the package

If the failing machine still cannot do the direct ADBC SSL test while the
working machine can, then `db2pq` is not the problem. The fix needs to happen
in the local `adbcpostgresql` / PostgreSQL client environment.

## 7. If you want to force a local source build

On macOS with Homebrew:

```sh
brew install libpq openssl@3 pkg-config
export PATH="/opt/homebrew/opt/libpq/bin:$PATH"
export PKG_CONFIG_PATH="/opt/homebrew/opt/libpq/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig:$PKG_CONFIG_PATH"
```

Then reinstall from source in R:

```r
remove.packages("adbcpostgresql")
install.packages(
  "adbcpostgresql",
  repos = c("https://apache.r-universe.dev", "https://cloud.r-project.org"),
  type = "source"
)
```

Then rerun the direct ADBC SSL connection test in section 2.
