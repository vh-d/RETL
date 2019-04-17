
#' is function vectorized over list of objects (not classes)
#' @param x list of objects
#' @param class_name name of the class
#'
#' @export
is_apply <- function(x, class_name) {
  if (!is.list(x)) {
    is(x, class_name)
  } else {
    sapply(x, is, class2 = class_name, USE.NAMES = TRUE)
  }
}

#' is function vectorized over list of objects and vector of classes
#' @param x list of objects
#' @param class_names name of the class
#' @details
#' For each object in the list, it is checked againts all the class names given. If any of the test is successfull it returns TRUE for given object.
#'
#' @export
is_apply2 <- function(x, class_names) {
  if (!length(x)) return(logical())
  res <- sapply(class_names, is_apply, x = x, USE.NAMES = TRUE)
  if (is.matrix(res)) return(apply(res, 1, any)) else {
    res <- any(res)
    names(res) <- names(x)

    return(res)
  }
}


#' convert all columns of specified type to another type
#' @rdname convert_cols
#' @export
convert_cols <- function(x, ...) {
  UseMethod(object = x, generic = "convert_cols")
}

#' @param x an input data.table
#'
#' @param from_class original type/class
#' @param to_class target type/class
#'
#' @details
#' Modifies the referenced data.tables!
#' @rdname convert_cols
#' @export
convert_cols.data.table <- function(x, from_class, to_class, inplace = TRUE){
  
  which_cols <- names(which(is_apply(x, from_class)))
  if (!length(which_cols)) return(x)
  if (!isTRUE(inplace)) x <- copy(x)
  x[, (which_cols) := lapply(.SD, as, Class = to_class), .SDcols = which_cols][]

  return(x)
}



#' Convert all Date and POSIXct columns to character columns
#' @param x a data.table object
#'
#' @param ... args passed to `as.Date()`
#' @param inplace Should x be modifed by reference? If FALSE a modified copy is returned.
#'
#' @export
Date2Char <- function(x, ..., inplace = TRUE) {
  
  cols <- names(which(sapply(colnames(x), function(column) is(x[[column]], "Date") || is(x[[column]], "POSIXct")), useNames = TRUE))
  if (!length(cols)) return(x) 
  if (!isTRUE(inplace)) x <- copy(x)
  x[, (cols) := lapply(cols, function(var_name) as.character(as.Date(get(var_name), ...)))]

  x
}

#' Convert all integer64 columns to numeric
#' @param x a data.table object
#'
#' @param ... args passed to `as.numeric()`
#' @param inplace Should x be modifed by reference? If FALSE a modified copy is returned.
#'
#' @export
int64_to_numeric <- function(x, ..., inplace = TRUE) {
  
  cols <- names(which(sapply(colnames(x), function(column) is(x[[column]], "integer64")), useNames = TRUE))
  if (!length(cols)) return(x) 
  if (!isTRUE(inplace)) x <- copy(x)
  x[, (cols) := lapply(cols, function(var_name) as.numeric(get(var_name), ...))]

  x
}

