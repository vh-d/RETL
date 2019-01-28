context("Pipes")

if (!requireNamespace("DBI")) stop("DBI package required!")
if (!requireNamespace("RSQLite")) stop("RSQLite package required!")

library(DBI)
library(data.table)

options(stringsAsFactors = FALSE)

dbcon <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
df1 <- data.frame(id = 1:10, dim = letters[1:10], value = rnorm(10))
tmpcsv <- tempfile(fileext = ".csv")

test_that("writing tables into a CSV file", {
  etl_write(to = tmpcsv, x = df1)
  expect_true(file.exists(tmpcsv))
})

test_that("reading tables from a CSV file", {
  
  dfcsv <- as.data.frame(etl_read(from = tmpcsv))
  expect_equivalent(dfcsv, df1)
  
  dfcsv2<- read.csv(file = tmpcsv)
  expect_equivalent(dfcsv2, df1)
  
})

test_that("loading tables into a DB", {
  etl_write(to = dbcon, x = df1, name = "df1")
  expect_true("df1" %in% DBI::dbListTables(dbcon))
  expect_error(etl_write(to = dbcon, x = df1, name = "df1"), "exists")
})

test_that("extracting tables from a DB", {
  df2 <- etl_read(from = dbcon, name = "df1", asDT = FALSE, lowercase = FALSE)
  expect_equal(df2, df1)
  expect_error(etl_read(from = dbcon, name = "df", asDT = FALSE, lowercase = FALSE), "no such table")
})


DBI::dbDisconnect(dbcon)
