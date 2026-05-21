# Resolve an Arrow data type from a string or pass through an existing type

Converts a user-friendly type name string to the corresponding Arrow
`DataType` object. If an Arrow `DataType` is passed directly it is
returned unchanged, so the function is safe to call on values that may
already be resolved.

## Usage

``` r
arrow_type(x)
```

## Arguments

- x:

  A string naming an Arrow type (see table above), or an Arrow
  `DataType` object.

## Value

An Arrow `DataType` object.

## Details

The following string names are recognised:

|  |  |
|----|----|
| **String** | **Arrow type** |
| `"int8"` | [`arrow::int8()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"int16"` | [`arrow::int16()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"int32"` | [`arrow::int32()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"int64"` | [`arrow::int64()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"uint8"` | [`arrow::uint8()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"uint16"` | [`arrow::uint16()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"uint32"` | [`arrow::uint32()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"uint64"` | [`arrow::uint64()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"float16"` | [`arrow::float16()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"float32"` | [`arrow::float32()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"float64"` | [`arrow::float64()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"double"` | [`arrow::float64()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"bool"` | [`arrow::boolean()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"boolean"` | [`arrow::boolean()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"string"` | [`arrow::utf8()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"utf8"` | [`arrow::utf8()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"large_string"` | [`arrow::large_utf8()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"large_utf8"` | [`arrow::large_utf8()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"date"` | [`arrow::date32()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"date32"` | [`arrow::date32()`](https://arrow.apache.org/docs/r/reference/data-type.html) |
| `"timestamp"` | `arrow::timestamp("us")` |
| `"timestamptz"` | `arrow::timestamp("us", timezone = "UTC")` |

## Examples

``` r
if (FALSE) { # \dontrun{
arrow_type("int32")$ToString()    # "int32"
arrow_type("double")$ToString()   # "double"
arrow_type("date")$ToString()     # "date32"

# Passing an already-resolved type is a no-op
arrow_type(arrow::float32())$ToString()
} # }
```
