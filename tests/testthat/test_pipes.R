
# etl_read and etl_write --------------------------------------------------
context("Pipes: etl_read, etl_write")

if (!requireNamespace("DBI")) stop("DBI package required!")
if (!requireNamespace("RSQLite")) stop("RSQLite package required!")

library(data.table)

options(stringsAsFactors = FALSE)

dbcon <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
dt1 <- data.table(id = 1:10, dim = letters[1:10], value = rnorm(10))
tmpcsv <- tempfile(fileext = ".csv")

renv1 <- new.env()
renv2 <- new.env()

test_that("writing tables into a CSV file", {
  etl_write(to = tmpcsv, x = dt1)
  expect_true(file.exists(tmpcsv))
})

test_that("reading tables from a CSV file", {
  
  dfcsv <- etl_read(from = tmpcsv)
  expect_equivalent(dfcsv, dt1)
  
  dfcsv2<- read.csv(file = tmpcsv)
  expect_equivalent(dfcsv2, dt1)
  
})

test_that("loading tables into a DB", {
  etl_write(to = dbcon, x = dt1, name = "dt1")
  expect_true("dt1" %in% DBI::dbListTables(dbcon))
  expect_error(etl_write(to = dbcon, x = dt1, name = "dt1"), "exists")
})

test_that("extracting tables from a DB", {
  
  dt2 <- 
    etl_read(
      from = dbcon, 
      name = "dt1",
      asDT = TRUE, 
      lowercase = FALSE
    )
  
  expect_equal(dt2, dt1)
  expect_error(etl_read(from = dbcon, name = "df", asDT = FALSE, lowercase = FALSE), "no such table")
})


# pipe_table --------------------------------------------------------------

context("Pipes: pipe_table")

test_that("Pipe from file to R env", {
  
  pipe_table(
    name = "dt1", 
    from = tmpcsv, 
    to   = renv1)
  
  expect_true(exists("dt1", envir = renv1))
  expect_equal(renv1$dt1, dt1)
})


test_that("Pipe from DB to R env", {
  
  pipe_table(
    name = "dt1", 
    from = dbcon, 
    to = renv2
  )
  
  expect_true(exists("dt1", envir = renv2))
  expect_equal(renv2$dt1, dt1)
})


DBI::dbDisconnect(dbcon)
