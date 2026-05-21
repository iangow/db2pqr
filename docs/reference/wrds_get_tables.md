# List WRDS relations in a schema

List WRDS relations in a schema

## Usage

``` r
wrds_get_tables(schema, wrds_id = NULL, views = FALSE)
```

## Arguments

- schema:

  WRDS schema name.

- wrds_id:

  Optional WRDS username override.

- views:

  If `TRUE`, include views as well as base tables.

## Value

A character vector of relation names.
