## R CMD check results

This is a resubmission. In this version I have:

* quoted software and data format names in `Title` and `Description`;
* added a method reference with DOI to `Description`;
* added executable examples for exported functions;
* guarded examples requiring WRDS credentials with
  `wrds_credentials_available(prompt = FALSE)`; and
* added comments explaining examples that remain in `\dontrun{}` because they
  require external database connections or existing local data files.

Local checks:

* `devtools::document()`
* `devtools::test()`: 197 passed
* `devtools::check()`: 0 errors, 0 warnings, 0 notes

## Notes

WRDS examples that require network access and credentials are guarded by
`wrds_credentials_available(prompt = FALSE)`, which returns `FALSE` without
prompting when credentials are unavailable.
