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

test_that("db_to_pq float64 numeric_mode casts numeric columns for adbc transfer", {
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
      numeric_mode = "float64",
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

test_that("db_to_pq decimal numeric_mode repairs bounded PostgreSQL numerics", {
  local_con <- structure(list(), class = "TestConnection")

  restore_fields <- local_rebind(
    "dbListFields",
    function(conn, name, ...) c("gvkey", "prc", "open_numeric"),
    env = asNamespace("DBI")
  )
  on.exit(restore_fields(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      if (grepl("data_type    = 'numeric'", statement, fixed = TRUE)) {
        return(data.frame(
          column_name = c("prc", "open_numeric"),
          numeric_precision = c(11L, NA_integer_),
          numeric_scale = c(5L, NA_integer_)
        ))
      }
      stop("Unexpected query")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  plan <- db2pq:::.db_to_pq_plan(
    con = local_con,
    table_name = "dsf",
    schema = "crsp",
    rename = c(prc = "price"),
    numeric_mode = "decimal",
    tz = NULL
  )

  expect_identical(
    plan$sql,
    paste(
      'SELECT "gvkey", CAST("prc" AS TEXT) AS "price",',
      'CAST("open_numeric" AS TEXT) AS "open_numeric" FROM "crsp"."dsf"'
    )
  )
  expect_identical(names(plan$col_types), "price")
  expect_match(plan$col_types$price$ToString(), "^decimal")
  expect_match(plan$col_types$price$ToString(), "\\(11, 5\\)$")
})

test_that("string-backed PostgreSQL numerics can cast to Arrow decimals", {
  tab <- arrow::Table$create(data.frame(
    prc = c("123.45000", "-0.01000"),
    stringsAsFactors = FALSE
  ))
  schemas <- db2pq:::.build_arrow_schemas(
    tab$schema,
    col_types = list(prc = arrow::decimal(precision = 11L, scale = 5L))
  )

  out <- db2pq:::.coerce_arrow_table(tab, schemas$data_schema)

  expect_match(out$schema$GetFieldByName("prc")$type$ToString(), "^decimal")
  expect_match(out$schema$GetFieldByName("prc")$type$ToString(), "\\(11, 5\\)$")
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

test_that("adbc postgres driver selection honors drivermanager override", {
  skip_if_not_installed("adbi")
  skip_if_not_installed("adbcdrivermanager")

  old_backend <- Sys.getenv("DB2PQ_ADBC_POSTGRES_DRIVER_BACKEND", unset = NA_character_)
  old_driver <- Sys.getenv("ADBC_POSTGRESQL_DRIVER", unset = NA_character_)
  Sys.setenv(
    DB2PQ_ADBC_POSTGRES_DRIVER_BACKEND = "drivermanager",
    ADBC_POSTGRESQL_DRIVER = "postgresql"
  )
  on.exit({
    if (is.na(old_backend)) Sys.unsetenv("DB2PQ_ADBC_POSTGRES_DRIVER_BACKEND") else Sys.setenv(DB2PQ_ADBC_POSTGRES_DRIVER_BACKEND = old_backend)
    if (is.na(old_driver)) Sys.unsetenv("ADBC_POSTGRESQL_DRIVER") else Sys.setenv(ADBC_POSTGRESQL_DRIVER = old_driver)
  }, add = TRUE)

  restore_package_available <- local_rebind(
    ".adbc_postgres_package_available",
    function() TRUE,
    env = asNamespace("db2pq")
  )
  on.exit(restore_package_available(), add = TRUE)

  restore_drivermanager_driver <- local_rebind(
    ".adbc_postgres_drivermanager_driver",
    function() adbcdrivermanager::adbc_driver_void(),
    env = asNamespace("db2pq")
  )
  on.exit(restore_drivermanager_driver(), add = TRUE)

  drv <- db2pq:::.adbc_postgres_dbi_driver()

  expect_s4_class(drv, "AdbiDriver")
  expect_true(inherits(slot(drv, "driver"), "adbc_driver_void"))
})

test_that("connect_adbc_postgres routes uri through selected ADBC driver", {
  fake_driver <- structure(list(), class = "FakeAdbiDriver")

  restore_driver <- local_rebind(
    ".adbc_postgres_dbi_driver",
    function() fake_driver,
    env = asNamespace("db2pq")
  )
  on.exit(restore_driver(), add = TRUE)

  restore_db_connect <- local_rebind(
    "dbConnect",
    function(drv, ..., uri) {
      expect_identical(drv, fake_driver)
      expect_identical(uri, "postgresql://user:pass@example.test/wrds")
      "connected"
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_db_connect(), add = TRUE)

  expect_identical(
    db2pq:::.connect_adbc_postgres("postgresql://user:pass@example.test/wrds"),
    "connected"
  )
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

test_that("db_to_pq planner applies rename to select list and col_types", {
  local_con <- structure(list(), class = "TestConnection")

  restore_fields <- local_rebind(
    "dbListFields",
    function(conn, name, ...) c("gvkey", "conm", "prc"),
    env = asNamespace("DBI")
  )
  on.exit(restore_fields(), add = TRUE)

  restore_get_query <- local_rebind(
    "dbGetQuery",
    function(conn, statement, params = NULL, ...) {
      if (grepl("data_type    = 'numeric'", statement, fixed = TRUE)) {
        return(data.frame(column_name = "prc"))
      }
      if (grepl("data_type    = 'timestamp without time zone'", statement, fixed = TRUE)) {
        return(data.frame(column_name = character()))
      }
      stop("Unexpected query")
    },
    env = asNamespace("DBI")
  )
  on.exit(restore_get_query(), add = TRUE)

  plan <- db2pq:::.db_to_pq_plan(
    con = local_con,
    table_name = "company",
    schema = "comp",
    rename = c(conm = "company_name", prc = "price"),
    col_types = list(company_name = "string"),
    transfer_method = "adbc",
    tz = NULL
  )

  expect_identical(
    plan$sql,
    'SELECT "gvkey", "conm" AS "company_name", CAST("prc" AS TEXT) AS "price" FROM "comp"."company"'
  )
  expect_identical(plan$output_names, c("gvkey", "company_name", "price"))
  expect_identical(names(plan$col_types), "company_name")
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
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop, rename,
             alt_table_name, chunk_size, transfer_method, numeric_mode, con, metadata,
             col_types, tz) {
      expect_identical(table_name, "dsi")
      expect_identical(schema, "crsp")
      expect_identical(out_file, "out.parquet")
      expect_identical(chunk_size, 100000L)
      expect_identical(transfer_method, "dbi")
      expect_identical(numeric_mode, "decimal")
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
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop, rename,
             alt_table_name, chunk_size, transfer_method, numeric_mode, con, metadata,
             col_types, tz) {
      expect_identical(transfer_method, "adbc")
      expect_identical(chunk_size, 250000L)
      expect_identical(numeric_mode, "decimal")
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
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop, rename,
             alt_table_name, chunk_size, transfer_method, numeric_mode, con, metadata,
             col_types, tz) {
      expect_identical(transfer_method, "adbc")
      expect_identical(chunk_size, 12345L)
      expect_identical(numeric_mode, "decimal")
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
    function(table_name, schema, data_dir, out_file, where, obs, keep, drop, rename,
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

test_that("wrds_schema_to_pq can discover WRDS views", {
  restore_get_tables <- local_rebind(
    "wrds_get_tables",
    function(schema, wrds_id = NULL, views = FALSE) {
      expect_identical(schema, "ff")
      expect_identical(wrds_id, "wrds-user")
      expect_true(views)
      "factors_monthly"
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_get_tables(), add = TRUE)

  restore_update_pq <- local_rebind(
    "wrds_update_pq",
    function(table_name, schema, data_dir, force, chunk_size, wrds_id,
             transfer_method, numeric_mode, ...) {
      expect_identical(table_name, "factors_monthly")
      expect_identical(schema, "ff")
      expect_identical(wrds_id, "wrds-user")
      "ff/factors_monthly.parquet"
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_update_pq(), add = TRUE)

  expect_identical(
    db2pq::wrds_schema_to_pq("ff", views = TRUE, wrds_id = "wrds-user"),
    list(factors_monthly = "ff/factors_monthly.parquet")
  )
})

test_that("wrds_schema_to_pq suppresses empty child messages", {
  restore_get_tables <- local_rebind(
    "wrds_get_tables",
    function(schema, wrds_id = NULL, views = FALSE) "factors_daily",
    env = asNamespace("db2pq")
  )
  on.exit(restore_get_tables(), add = TRUE)

  restore_update_pq <- local_rebind(
    "wrds_update_pq",
    function(table_name, schema, ...) {
      message("")
      message("\n")
      message(schema, ".", table_name, " already up to date.")
      NULL
    },
    env = asNamespace("db2pq")
  )
  on.exit(restore_update_pq(), add = TRUE)

  messages <- character()
  stdout <- capture.output(
    withCallingHandlers(
      db2pq::wrds_schema_to_pq("ff", views = TRUE),
      message = function(m) {
        messages <<- c(messages, conditionMessage(m))
        invokeRestart("muffleMessage")
      }
    ),
    type = "output"
  )

  expect_identical(
    messages,
    "Processing 1 table(s) in schema 'ff'.\n"
  )
  expect_identical(stdout, "ff.factors_daily already up to date.")
})
