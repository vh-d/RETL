context("Tools")

library(etltools)

test_that("is_apply() result length", {
  expect_length(is_apply(list(),   "character"), 0)
  expect_length(is_apply(list(""), "character"), 1)
  expect_length(is_apply(NULL,     "character"), 1) # is_apply's fallback to is() for object that are not lists
})

test_that("is_apply() result correctness", {
  expect_true(is_apply("", "character"))
  expect_true(is_apply(1L, "integer"))
  expect_true(is_apply(list(NULL), "NULL"))
  expect_equal(is_apply(list("", 1L, as.Date(NA)), "character"), c(TRUE, FALSE, FALSE))
})


test_that("is_apply2() behaviour", {
  expect_true(is_apply(1L, "integer"))
  expect_true(is_apply(list(NULL), "NULL"))
  expect_equal(is_apply2(list(1), c("integer", "character")),  FALSE)
  expect_equal(is_apply2(list(1L), c("integer", "character")), TRUE)
  expect_named(is_apply2(list(a = 1), "character"))
  expect_equal(is_apply2(list(a = 1), c("integer", "character")), c(a = FALSE))
  expect_equal(is_apply2(list(a = 1L), c("integer", "character")), c(a = TRUE))
  expect_equal(is_apply2(list("", 1L, as.Date(NA), 1), c("character", "integer", "Date")), c(TRUE, TRUE, TRUE, FALSE))
})

test_that("is_apply2() extreme cases", {
  expect_length(is_apply2(NULL, "integer"), 0)
  expect_error(is_apply2())
  expect_error(is_apply2(1L))
  expect_error(is_apply2(1L, ""))
})

