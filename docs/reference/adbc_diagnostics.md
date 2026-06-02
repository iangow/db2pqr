# Report optional ADBC dependency status

Report optional ADBC dependency status

## Usage

``` r
adbc_diagnostics()
```

## Value

A tibble with installed status and package versions for optional ADBC
packages used by `db2pq`.

## Examples

``` r
adbc_diagnostics()
#> # A tibble: 4 × 4
#>   package           installed version  role                                     
#>   <chr>             <lgl>     <chr>    <chr>                                    
#> 1 adbi              TRUE      0.1.2    DBI adapter for ADBC drivers             
#> 2 adbcpostgresql    TRUE      0.23.0   ADBC PostgreSQL driver                   
#> 3 adbcdrivermanager TRUE      0.23.0.1 Optional driver manager backend          
#> 4 nanoarrow         TRUE      0.8.0    Arrow chunk inspection used by ADBC diag…
```
