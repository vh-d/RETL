#' @title Make all data.table names lowercase
#' @param DT
#'
#' @rdname lowercase_names
#' @export
lowercase_names.data.table <- function(DT, inplace = FALSE) {

  if (inplace) {
    setnames(DT, tolower(colnames(DT)))
  } else {
    DT <- copy(DT)
    colnames(DT) <- tolower(colnames(DT))
  }

  return(DT)
}


#' @rdname lowercase_names
#' @export
lowercase_names <- function(x, ...) {
  UseMethod(object = x, generic = "lowercase_names")
}
