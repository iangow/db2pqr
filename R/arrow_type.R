# Lookup table: user-friendly string -> Arrow type object.
# Used by arrow_type() and referenced in col_types documentation.
.ARROW_TYPE_MAP <- list(
  # Signed integers
  int8    = arrow::int8(),
  int16   = arrow::int16(),
  int32   = arrow::int32(),
  int64   = arrow::int64(),
  # Unsigned integers
  uint8   = arrow::uint8(),
  uint16  = arrow::uint16(),
  uint32  = arrow::uint32(),
  uint64  = arrow::uint64(),
  # Floating point
  float16 = arrow::float16(),
  float32 = arrow::float32(),
  float64 = arrow::float64(),
  double  = arrow::float64(),   # common alias
  # Boolean
  bool    = arrow::boolean(),
  boolean = arrow::boolean(),
  # Strings
  string        = arrow::utf8(),
  utf8          = arrow::utf8(),
  large_string  = arrow::large_utf8(),
  large_utf8    = arrow::large_utf8(),
  # Dates
  date   = arrow::date32(),
  date32 = arrow::date32(),
  # Timestamps (microsecond resolution, matching PostgreSQL)
  timestamp   = arrow::timestamp("us"),
  timestamptz = arrow::timestamp("us", timezone = "UTC")
)

#' Resolve an Arrow data type from a string or pass through an existing type
#'
#' Converts a user-friendly type name string to the corresponding Arrow
#' \code{DataType} object. If an Arrow \code{DataType} is passed directly it is
#' returned unchanged, so the function is safe to call on values that may
#' already be resolved.
#'
#' The following string names are recognised:
#'
#' \tabular{ll}{
#'   \strong{String}    \tab \strong{Arrow type}  \cr
#'   \code{"int8"}      \tab \code{arrow::int8()}      \cr
#'   \code{"int16"}     \tab \code{arrow::int16()}     \cr
#'   \code{"int32"}     \tab \code{arrow::int32()}     \cr
#'   \code{"int64"}     \tab \code{arrow::int64()}     \cr
#'   \code{"uint8"}     \tab \code{arrow::uint8()}     \cr
#'   \code{"uint16"}    \tab \code{arrow::uint16()}    \cr
#'   \code{"uint32"}    \tab \code{arrow::uint32()}    \cr
#'   \code{"uint64"}    \tab \code{arrow::uint64()}    \cr
#'   \code{"float16"}   \tab \code{arrow::float16()}   \cr
#'   \code{"float32"}   \tab \code{arrow::float32()}   \cr
#'   \code{"float64"}   \tab \code{arrow::float64()}   \cr
#'   \code{"double"}    \tab \code{arrow::float64()}   \cr
#'   \code{"bool"}      \tab \code{arrow::boolean()}   \cr
#'   \code{"boolean"}   \tab \code{arrow::boolean()}   \cr
#'   \code{"string"}    \tab \code{arrow::utf8()}      \cr
#'   \code{"utf8"}      \tab \code{arrow::utf8()}      \cr
#'   \code{"large_string"} \tab \code{arrow::large_utf8()} \cr
#'   \code{"large_utf8"}   \tab \code{arrow::large_utf8()} \cr
#'   \code{"date"}         \tab \code{arrow::date32()}                          \cr
#'   \code{"date32"}       \tab \code{arrow::date32()}                          \cr
#'   \code{"timestamp"}    \tab \code{arrow::timestamp("us")}                   \cr
#'   \code{"timestamptz"}  \tab \code{arrow::timestamp("us", timezone = "UTC")}
#' }
#'
#' @param x A string naming an Arrow type (see table above), or an Arrow
#'   \code{DataType} object.
#'
#' @return An Arrow \code{DataType} object.
#'
#' @examples
#' arrow_type("int32")    # arrow::int32()
#' arrow_type("double")   # arrow::float64()
#' arrow_type("date")     # arrow::date32()
#'
#' # Passing an already-resolved type is a no-op
#' arrow_type(arrow::float32())
#'
#' @export
# Returns TRUE if an Arrow DataType is a timestamp type.
.is_arrow_timestamp <- function(type) {
  inherits(type, "Timestamp")
}

arrow_type <- function(x) {
  if (inherits(x, "DataType")) return(x)
  if (!is.character(x) || length(x) != 1L) {
    stop("`col_types` values must be a string type name or an Arrow DataType object.")
  }
  resolved <- .ARROW_TYPE_MAP[[x]]
  if (is.null(resolved)) {
    stop("Unknown Arrow type name: \"", x, "\". ",
         "See ?arrow_type for supported names.")
  }
  resolved
}
