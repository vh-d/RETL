
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


#' @export
Date2Char <- function(dt) {
  cols <- names(which(sapply(colnames(dt), function(column) is(dt[[column]], "Date") || is(dt[[column]], "POSIXct")), useNames = TRUE))
  if (length(cols) > 0) dt[, (cols) := lapply(cols, function(x) as.character(as.Date(get(x))))]

  dt
}

#' @export
int64_to_numeric <- function(dt) {
  cols <- names(which(sapply(colnames(dt), function(column) is(dt[[column]], "integer64")), useNames = TRUE))
  if (length(cols) > 0) dt[, (cols) := lapply(cols, function(x) as.numeric(get(x)))]

  dt
}

