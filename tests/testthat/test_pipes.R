context("Pipes")

if (!requireNamespace("DBI")) stop("DBI package required!")
if (!requireNamespace("RSQLite")) stop("DBI package required!")

library(DBI)

dbcon <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
df1 <- data.frame(id = 1:10, value = rnorm(10))

test_that("loading tables into a DB", {
  etl_write(to = dbcon, x = df1, name = "df1")
  expect_true("df1" %in% DBI::dbListTables(dbcon))
  expect_error(etl_write(to = dbcon, x = df1, name = "df1"), "exists")
})

test_that("loading tables into a DB", {
  df2 <- etl_read(from = dbcon, name = "df1", asDT = FALSE, lowercase = FALSE)
  expect_equal(df2, df1)
  expect_error(etl_read(from = dbcon, name = "df", asDT = FALSE, lowercase = FALSE), "no such table")
})


DBI::dbDisconnect(dbcon)
