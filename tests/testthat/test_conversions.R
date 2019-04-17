context("Converting classes of column in a table")

library(data.table)

test_that("Columns of data.table get converted with pass-by-reference", {
  DT1 <- data.table(id = 1L:10L, date = c(as.Date("2018-12-31"), NA), value = rnorm(10))
  DT2 <- DT1[, .(id, date = as.character(date), value)]
  DT3 <- convert_cols(DT1, "Date", "character", inplace = TRUE)
  expect_identical(DT2, DT3)
  expect_identical(DT1, DT3)
})

test_that("Columns of data.table get converted with copy-on-modify semantics", {
  DT1 <- data.table(id = 1L:10L, date = c(as.Date("2018-12-31"), NA), value = rnorm(10))
  DT2 <- DT1[, .(id, date = as.character(date), value)]
  DT3 <- convert_cols(DT1, "Date", "character", inplace = FALSE)
  expect_false(is.character(DT1$date), info = "date remains of 'Date' data type")
  expect_true(is(DT1$date, "Date"), info = "date remains of 'Date' data type")
  expect_true(identical(DT2, DT3))
  expect_false(identical(DT1, DT3))
})

if (requireNamespace("bit64"))
  test_that("int64 type can be converted to numeric", {
    DT  <- data.table(x = bit64::as.integer64(c(1e10, 1e10+1)))
    DT2 <- RETL::int64_to_numeric(x = DT, inplace = FALSE)
    expect_true(is.numeric(DT2$x))
    expect_false(bit64::is.integer64(DT2))
  })
