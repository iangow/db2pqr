local_rebind <- function(name, value, env) {
  old <- get(name, envir = env, inherits = FALSE)
  unlockBinding(name, env)
  assign(name, value, envir = env)
  lockBinding(name, env)

  function() {
    unlockBinding(name, env)
    assign(name, old, envir = env)
    lockBinding(name, env)
  }
}

test_that("db_to_pq routes ADBC transfer through sql_to_pq_dev", {
  local_con <- structure(list(), class = "TestConnection")

  restore_fields <- local_rebind(
    "dbListFields",
    function(conn, name, ...) {
      expect_identical(conn, local_con)
      expect_identical(unname(name@name), c("demo", "test_table"))
      c("x", "y")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_fields(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      if (grepl("data_type    = 'numeric'", statement, fixed = TRUE)) {
        return(data.frame(column_name = character()))
      }
      if (grepl("data_type    = 'timestamp without time zone'", statement, fixed = TRUE)) {
        return(data.frame(column_name = character()))
      }
      stop("Unexpected query")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_sql_to_pq_dev <- local_rebind(
    "sql_to_pq_dev",
    function(con, sql, out_file, chunk_size, metadata, col_types) {
      expect_identical(con, local_con)
      expect_identical(sql, 'SELECT "x", "y" FROM "demo"."test_table"')
      expect_identical(out_file, "out.parquet")
      expect_identical(chunk_size, 42L)
      expect_identical(metadata, list(source = "test"))
      expect_null(col_types)

      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_sql_to_pq_dev(), add = TRUE)

  expect_identical(
    db2pq:::db_to_pq(
      table_name = "test_table",
      schema = "demo",
      out_file = "out.parquet",
      chunk_size = 42L,
      transfer_method = "adbc",
      con = local_con,
      metadata = list(source = "test"),
      tz = NULL
    ),
    "out.parquet"
  )
})

test_that("db_to_pq casts numeric columns for adbc transfer", {
  local_con <- structure(list(), class = "TestConnection")

  restore_fields <- local_rebind(
    "dbListFields",
    function(conn, name, ...) c("prc", "ret", "date"),
    env = asNamespace("DBI")
  )
  on.exit(restore_fields(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      if (grepl("data_type    = 'numeric'", statement, fixed = TRUE)) {
        return(data.frame(column_name = c("prc", "ret")))
      }
      if (grepl("data_type    = 'timestamp without time zone'", statement, fixed = TRUE)) {
        return(data.frame(column_name = character()))
      }
      stop("Unexpected query")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_sql_to_pq_dev <- local_rebind(
    "sql_to_pq_dev",
    function(con, sql, out_file, chunk_size, metadata, col_types) {
      expect_match(sql, 'CAST\\("prc" AS DOUBLE PRECISION\\) AS "prc"')
      expect_match(sql, 'CAST\\("ret" AS DOUBLE PRECISION\\) AS "ret"')
      expect_match(sql, '"date"')
      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_sql_to_pq_dev(), add = TRUE)

  expect_identical(
    db2pq:::db_to_pq(
      table_name = "test_table",
      schema = "demo",
      out_file = "out.parquet",
      transfer_method = "adbc",
      con = local_con,
      tz = NULL
    ),
    "out.parquet"
  )
})

test_that("db_to_pq raw numeric_mode skips adbc numeric casts", {
  local_con <- structure(list(), class = "TestConnection")

  restore_fields <- local_rebind(
    "dbListFields",
    function(conn, name, ...) c("prc", "ret", "date"),
    env = asNamespace("DBI")
  )
  on.exit(restore_fields(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      if (grepl("data_type    = 'numeric'", statement, fixed = TRUE)) {
        return(data.frame(column_name = c("prc", "ret")))
      }
      if (grepl("data_type    = 'timestamp without time zone'", statement, fixed = TRUE)) {
        return(data.frame(column_name = character()))
      }
      stop("Unexpected query")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_sql_to_pq_dev <- local_rebind(
    "sql_to_pq_dev",
    function(con, sql, out_file, chunk_size, metadata, col_types) {
      expect_no_match(sql, 'CAST\\("prc" AS DOUBLE PRECISION\\) AS "prc"')
      expect_no_match(sql, 'CAST\\("ret" AS DOUBLE PRECISION\\) AS "ret"')
      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_sql_to_pq_dev(), add = TRUE)

  expect_identical(
    db2pq:::db_to_pq(
      table_name = "test_table",
      schema = "demo",
      out_file = "out.parquet",
      transfer_method = "adbc",
      numeric_mode = "raw",
      con = local_con,
      tz = NULL
    ),
    "out.parquet"
  )
})

test_that("adbc_numeric_cast_map_from_column_info finds string-like result columns", {
  info <- data.frame(
    name = c("date", "vwretd", "totcnt", "usdval"),
    type = c("date32", "string", "int32", "utf8"),
    stringsAsFactors = FALSE
  )

  out <- db2pq:::.adbc_numeric_cast_map_from_column_info(info)

  expect_identical(out, c("vwretd", "usdval"))
})

test_that("rewrite_select_list applies numeric casts and timestamp expressions", {
  out <- db2pq:::.rewrite_select_list(
    columns = c("date", "prc", "ret"),
    numeric_cast_cols = c("prc", "ret"),
    timestamp_exprs = c(date = '("date" AT TIME ZONE \'UTC\') AS "date"')
  )

  expect_identical(
    out,
    c(
      '("date" AT TIME ZONE \'UTC\') AS "date"',
      'CAST("prc" AS DOUBLE PRECISION) AS "prc"',
      'CAST("ret" AS DOUBLE PRECISION) AS "ret"'
    )
  )
})

test_that("wrds_update_pq forwards transfer_method to db_to_pq", {
  local_con <- structure(list(), class = "TestConnection")

  restore_wrds_connect <- local_rebind(
    "wrds_connect",
    function(...) local_con,
    env = asNamespace("wrds")
  )
  on.exit(restore_wrds_connect(), add = TRUE)

  restore_disconnect <- local_rebind(
    "dbDisconnect",
    function(conn, ...) {
      expect_identical(conn, local_con)
      invisible(TRUE)
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_disconnect(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      expect_identical(conn, local_con)
      expect_match(statement, "obj_description")
      expect_identical(params, list("crsp.dsi"))
      data.frame(comment = "Last modified: 03/15/2024 14:30:00")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_get_pq_date <- local_rebind(
    ".get_pq_date",
    function(path) {
      expect_identical(path, "out.parquet")
      NULL
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_get_pq_date(), add = TRUE)

  restore_db_to_pq <- local_rebind(
    "db_to_pq",
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop,
             alt_table_name, chunk_size, transfer_method, numeric_mode, con, metadata,
             col_types, tz) {
      expect_identical(table_name, "dsi")
      expect_identical(schema, "crsp")
      expect_identical(out_file, "out.parquet")
      expect_identical(chunk_size, 100000L)
      expect_identical(transfer_method, "dbi")
      expect_identical(numeric_mode, "float64")
      expect_identical(con, local_con)
      expect_identical(metadata, list(last_modified = "Last modified: 03/15/2024 14:30:00"))
      expect_null(col_types)
      expect_identical(tz, "UTC")

      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_db_to_pq(), add = TRUE)

  expect_identical(
    db2pq::wrds_update_pq(
      table_name = "dsi",
      schema = "crsp",
      out_file = "out.parquet",
      transfer_method = "dbi"
    ),
    "out.parquet"
  )
})

test_that("wrds_update_pq uses native ADBC connection for adbc transfer", {
  adbc_con <- structure(list(), class = c("AdbiConnection", "TestConnection"))

  restore_wrds_connect_adbc <- local_rebind(
    "wrds_connect_adbc",
    function(wrds_id = NULL) {
      expect_null(wrds_id)
      adbc_con
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_wrds_connect_adbc(), add = TRUE)

  restore_disconnect <- local_rebind(
    "dbDisconnect",
    function(conn, ...) {
      expect_identical(conn, adbc_con)
      invisible(TRUE)
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_disconnect(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      expect_identical(conn, adbc_con)
      expect_match(statement, "obj_description")
      data.frame(comment = "Last modified: 03/15/2024 14:30:00")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_get_pq_date <- local_rebind(
    ".get_pq_date",
    function(path) NULL,
    env = asNamespace("db2pq")
  )
  on.exit(restore_get_pq_date(), add = TRUE)

  restore_db_to_pq <- local_rebind(
    "db_to_pq",
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop,
             alt_table_name, chunk_size, transfer_method, numeric_mode, con, metadata,
             col_types, tz) {
      expect_identical(transfer_method, "adbc")
      expect_identical(chunk_size, 250000L)
      expect_identical(numeric_mode, "float64")
      expect_identical(con, adbc_con)
      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_db_to_pq(), add = TRUE)

  expect_identical(
    db2pq::wrds_update_pq(
      table_name = "dsi",
      schema = "crsp",
      out_file = "out.parquet",
      transfer_method = "adbc"
    ),
    "out.parquet"
  )
})

test_that("wrds_update_pq respects explicit chunk_size override for adbc", {
  adbc_con <- structure(list(), class = c("AdbiConnection", "TestConnection"))

  restore_wrds_connect_adbc <- local_rebind(
    "wrds_connect_adbc",
    function(wrds_id = NULL) adbc_con,
    env = asNamespace("db2pq")
  )
  on.exit(restore_wrds_connect_adbc(), add = TRUE)

  restore_disconnect <- local_rebind(
    "dbDisconnect",
    function(conn, ...) invisible(TRUE),
    env = asNamespace("DBI")
  )
  on.exit(restore_disconnect(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      data.frame(comment = "Last modified: 03/15/2024 14:30:00")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_get_pq_date <- local_rebind(
    ".get_pq_date",
    function(path) NULL,
    env = asNamespace("db2pq")
  )
  on.exit(restore_get_pq_date(), add = TRUE)

  restore_db_to_pq <- local_rebind(
    "db_to_pq",
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop,
             alt_table_name, chunk_size, transfer_method, numeric_mode, con, metadata,
             col_types, tz) {
      expect_identical(transfer_method, "adbc")
      expect_identical(chunk_size, 12345L)
      expect_identical(numeric_mode, "float64")
      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_db_to_pq(), add = TRUE)

  expect_identical(
    db2pq::wrds_update_pq(
      table_name = "dsi",
      schema = "crsp",
      out_file = "out.parquet",
      transfer_method = "adbc",
      chunk_size = 12345L
    ),
    "out.parquet"
  )
})

test_that("wrds_update_pq forwards raw numeric_mode for adbc", {
  adbc_con <- structure(list(), class = c("AdbiConnection", "TestConnection"))

  restore_wrds_connect_adbc <- local_rebind(
    "wrds_connect_adbc",
    function(wrds_id = NULL) adbc_con,
    env = asNamespace("db2pq")
  )
  on.exit(restore_wrds_connect_adbc(), add = TRUE)

  restore_disconnect <- local_rebind(
    "dbDisconnect",
    function(conn, ...) invisible(TRUE),
    env = asNamespace("DBI")
  )
  on.exit(restore_disconnect(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      data.frame(comment = "Last modified: 03/15/2024 14:30:00")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  restore_get_pq_date <- local_rebind(
    ".get_pq_date",
    function(path) NULL,
    env = asNamespace("db2pq")
  )
  on.exit(restore_get_pq_date(), add = TRUE)

  restore_db_to_pq <- local_rebind(
    "db_to_pq",
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop,
             alt_table_name, chunk_size, transfer_method, numeric_mode, con, metadata,
             col_types, tz) {
      expect_identical(transfer_method, "adbc")
      expect_identical(numeric_mode, "raw")
      invisible(out_file)
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_db_to_pq(), add = TRUE)

  expect_identical(
    db2pq::wrds_update_pq(
      table_name = "dsi",
      schema = "crsp",
      out_file = "out.parquet",
      transfer_method = "adbc",
      numeric_mode = "raw"
    ),
    "out.parquet"
  )
})
