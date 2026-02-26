# Retrieve the WRDS user ID.
#
# Mirrors the logic used by wrds::wrds_connect(): looks up the username
# stored in the system keyring under "wrds_user". The wrds_id argument,
# if supplied, takes precedence.
#
# @param wrds_id Optional override. If NULL, the keyring is consulted.
# @param user_key Keyring key name for the WRDS username.
# @return A non-empty character string.
# @keywords internal
.get_wrds_id <- function(wrds_id = NULL, user_key = "wrds_user") {
  if (!is.null(wrds_id)) return(wrds_id)
  tryCatch(
    keyring::key_get(user_key),
    error = function(e) {
      stop(
        "Could not retrieve WRDS username from keyring. ",
        "Run wrds::wrds_set_credentials() to set up credentials.",
        call. = FALSE
      )
    }
  )
}

# Execute SAS code on the WRDS server via SSH and return the output as a
# character vector of lines.
#
# Uses processx to shell out to the system `ssh` binary, which handles
# WRDS's two-step authentication (public key + keyboard-interactive)
# correctly. The SAS code is written to a temp file and passed as stdin
# to `qsas -stdio -noterminal`.
#
# @param sas_code SAS code to execute.
# @param wrds_id WRDS user ID. Defaults to the WRDS_ID environment variable.
# @return Character vector of stdout lines.
# @keywords internal
.sas_exec <- function(sas_code, wrds_id = NULL) {
  wrds_id <- .get_wrds_id(wrds_id)

  host   <- paste0(wrds_id, "@wrds-cloud-sshkey.wharton.upenn.edu")
  tmp_in <- tempfile(fileext = ".sas")
  on.exit(unlink(tmp_in), add = TRUE)
  writeLines(sas_code, tmp_in)

  result <- processx::run(
    "ssh",
    args            = c(host, "qsas -stdio -noterminal"),
    stdin           = tmp_in,
    error_on_status = FALSE
  )

  strsplit(result$stdout, "\n")[[1]]
}

# Run PROC CONTENTS on a SAS dataset on WRDS via SSH and return the output
# as a character vector of lines.
#
# @param table_name Table name.
# @param sas_schema SAS library (schema) name.
# @param wrds_id WRDS user ID. Defaults to the WRDS_ID environment variable.
# @param encoding Character encoding for the SAS dataset. Default "utf-8".
# @return Character vector of PROC CONTENTS output lines.
# @keywords internal
.proc_contents_sas <- function(table_name, sas_schema,
                               wrds_id = NULL, encoding = "utf-8") {
  sas_code <- sprintf(
    "PROC CONTENTS data=%s.%s(encoding='%s');",
    sas_schema, table_name, encoding
  )
  .sas_exec(sas_code, wrds_id = wrds_id)
}

# Parse the "Last Modified" string out of PROC CONTENTS output.
#
# Parses the stdout of PROC CONTENTS and returns a string of the form
# "Last modified: MM/DD/YYYY HH:MM:SS", matching the format used in
# PostgreSQL table comments on WRDS. Returns NULL if the date is not found.
#
# @param table_name Table name (used only in messages).
# @param sas_schema SAS library (schema) name.
# @param wrds_id WRDS user ID. Defaults to the WRDS_ID environment variable.
# @param encoding Character encoding for the SAS dataset.
# @return A character string like "Last modified: MM/DD/YYYY HH:MM:SS",
#   or NULL if not found.
# @keywords internal
.get_modified_str_sas <- function(table_name, sas_schema,
                                  wrds_id = NULL, encoding = "utf-8") {
  lines <- .proc_contents_sas(
    table_name, sas_schema,
    wrds_id = wrds_id, encoding = encoding
  )

  if (length(lines) == 0 || all(!nzchar(trimws(lines)))) {
    message("Table ", sas_schema, ".", table_name, " not found.")
    return(NULL)
  }

  modified <- ""
  next_row <- FALSE

  for (line in lines) {
    if (next_row) {
      line <- gsub("^\\s+(.*?)\\s*$", "\\1", line)
      if (!grepl("Protection", line)) {
        modified <- paste0(modified, " ", trimws(line, "right"))
      }
      next_row <- FALSE
    }
    if (grepl("^Last Modified", line)) {
      modified <- trimws(
        gsub("^Last Modified\\s+(.*?)\\s{2,}.*$", "Last modified: \\1", line,
             perl = TRUE),
        "right"
      )
      next_row <- TRUE
    }
  }

  if (nzchar(trimws(modified))) modified else NULL
}
