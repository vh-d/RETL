context("Converting classes of column in a table")

library(etltools)
library(data.table)

test_that("Columns of data.table get converted by reference", {
  DT1 <- data.table(id = 1L:10L, date = c(as.Date("2018-12-31"), NA), value = rnorm(10))
  DT2 <- DT1[, .(id, date = as.character(date), value)]
  DT3 <- etltools::convert_cols(DT1, "Date", "character")
  expect_identical(DT2, DT3)
})

