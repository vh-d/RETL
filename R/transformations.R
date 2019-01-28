
#' Add a new column as a differenced value of existing column
#'
#' @param .data input data.table
#' @param value_var character; name of the original column
#' @param by_vars character; vector of dimensions for 'group by'
#' @param postfix character; posfix is added at the end of the name of the new column
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
