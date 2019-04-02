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



#' @title Make all data.table names lowercase
#' @param x
#'
#' @rdname lowercase_names
#' @export
setnames_lowercase.data.table <- function(x, inplace = TRUE) {
  
  if (!isTRUE(inplace)) x <- copy(x)
  setnames(x, tolower(colnames(x)))
  
  return(x)
}


#' @rdname lowercase_names
#' @export
setnames_lowercase <- function(x, ...) {
  UseMethod(object = x, generic = "setnames_lowercase")
}
