# Resolve the Parquet data repository directory

Resolves the root directory used for Parquet data from an explicit
argument or the `DATA_DIR` environment variable. In an interactive
session, when neither is available, the helper asks the user to choose
or enter a directory and offers to persist it in either project-level
`.Renviron` or user-level `~/.Renviron`.

## Usage

``` r
pq_data_dir(data_dir = NULL, prompt = interactive(), fallback = ".")
```

## Arguments

- data_dir:

  Optional Parquet data repository root.

- prompt:

  If `TRUE`, prompt interactively when `data_dir` and `DATA_DIR` are
  both missing.

- fallback:

  Directory returned when prompting is disabled and no explicit or
  environment value is available.

## Value

A directory path as a character string.
