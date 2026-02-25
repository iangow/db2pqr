source(test_path("../../R/wrds_update_pq.R"))

# --- Format 1: "Last modified: MM/DD/YYYY HH:MM:SS" ---

test_that("parses standard 'Last modified' timestamp", {
  result <- .parse_wrds_date("Last modified: 03/15/2024 14:30:00")
  expect_equal(result, as.Date("2024-03-15"))
})

test_that("parses 'Last modified' timestamp at start of month", {
  result <- .parse_wrds_date("Last modified: 01/01/2020 00:00:00")
  expect_equal(result, as.Date("2020-01-01"))
})

test_that("parses 'Last modified' with leading whitespace", {
  result <- .parse_wrds_date("  Last modified: 11/30/2023 02:00:00")
  expect_equal(result, as.Date("2023-11-30"))
})

# --- Format 2: "(Updated YYYY-MM-DD)" ---

test_that("parses 'Updated' ISO date format", {
  result <- .parse_wrds_date("Compustat Fundamentals (Updated 2024-06-01)")
  expect_equal(result, as.Date("2024-06-01"))
})

test_that("returns NULL for bare ISO date not wrapped in (Updated ...)", {
  expect_null(.parse_wrds_date("2023-12-31"))
})

# --- Edge cases ---

test_that("returns NULL for NULL input", {
  expect_null(.parse_wrds_date(NULL))
})

test_that("returns NULL for NA input", {
  expect_null(.parse_wrds_date(NA_character_))
})

test_that("returns NULL for empty string", {
  expect_null(.parse_wrds_date(""))
})

test_that("returns NULL for unrecognised string", {
  expect_null(.parse_wrds_date("No date information available"))
})

test_that("returns NULL for (Updated ...) not at end of string", {
  expect_null(.parse_wrds_date("(Updated 2024-06-01) extra text"))
})

test_that("returns NULL for partial/malformed date", {
  expect_null(.parse_wrds_date("(Updated 2024-99)"))
})

# --- Format precedence: MM/DD/YYYY wins when both patterns present ---

test_that("MM/DD/YYYY format takes precedence when both patterns present", {
  result <- .parse_wrds_date("Last modified: 03/15/2024 00:00:00 (Updated 2023-01-01)")
  expect_equal(result, as.Date("2024-03-15"))
})
