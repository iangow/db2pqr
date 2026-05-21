# Resolve a WRDS username

Resolves a WRDS username from, in order, an explicit argument,
`WRDS_ID`, `WRDS_USER`, and the keyring entry written by
[`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html).

## Usage

``` r
wrds_get_username(wrds_id = NULL, user_key = "wrds_user")
```

## Arguments

- wrds_id:

  Optional WRDS username override.

- user_key:

  Keyring key used by the `wrds` package for the WRDS username.

## Value

A non-empty character string.
