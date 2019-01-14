
#' Add a new column as a differenced value of existing column
#'
#' @param .data
#' @param value_var
#' @param by_vars
#' @param postfix
#'
#' @return
#' @export
#'
#' @examples
add_diff_value <- function(.data, value_var = NULL, by_vars = NULL, postfix = "_diff") {
  if (length(by_vars)) {
    setkeyv(.data, by_vars)
    .data[, (paste0(value_var, postfix)) := diff_fill(get(value_var)), by = eval(by_vars[-length(by_vars)])]
  } else {
    .data[, (paste0(value_var, postfix)) := diff_fill(get(value_var))]
  }

  return(.data)
}


diff_fill <- function(x, lag = 1, differences = 1, fill = NA) {
  c(rep(fill, lag*differences), diff(x, lag = lag, differences = differences))
}
