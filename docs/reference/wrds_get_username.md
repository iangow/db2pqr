# Resolve a WRDS username

Resolves a WRDS username from, in order, an explicit argument,
`WRDS_ID`, `WRDS_USER`, and the keyring entry written by
[`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html).

## Usage

``` r
wrds_get_username(
  wrds_id = NULL,
  user_key = "wrds_user",
  prompt = interactive()
)
```

## Arguments

- wrds_id:

  Optional WRDS username override.

- user_key:

  Keyring key used by the `wrds` package for the WRDS username.

- prompt:

  If `TRUE`, prompt interactively for a WRDS username when no explicit,
  environment, or keyring username can be resolved.

## Value

A non-empty character string.

## Examples

``` r
wrds_get_username(wrds_id = "example_user")
#> [1] "example_user"
```
