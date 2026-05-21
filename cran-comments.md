## R CMD check results

This package is preparing for an initial CRAN submission.

Local checks to run before submission:

* `devtools::document()`
* `devtools::test()`
* `devtools::check()`
* `urlchecker::url_check()`
* rhub / GitHub Actions checks on macOS, Windows, and Linux

## Notes

Networked WRDS examples are wrapped in `\dontrun{}` because they require a WRDS
account and PostgreSQL credentials.
