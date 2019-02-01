#' Reads all sheets from an excel xlsx file into a list
#'
#' @param file file name
#' @param ... args passed to openxlsx::readWorkbook
#'
#' @return a list of data.frames
#' @export
excel_worksheets <- function(file, ...) {
  
  if (!requireNamespace("openxlsx")) stop("openxlsx package required!")
  
  wb <- openxlsx::loadWorkbook(file)
  sh_names <- openxlsx::sheets(wb)
  
  sheets <- 
    lapply(
      X   = sh_names, 
      FUN = openxlsx::readWorkbook, 
      # arguments passed to readWorkbook
      xlsxFile = wb, 
      ...
    )
  names(sheets) <- sh_names
  
  return(sheets)
}


#' Reads all sheets from an excel xlsx file into a list
#'
#' @param file file name
#'
#' @export
excel_worksheets_readxl <- function(file) {
  
  if (!requireNamespace("readxl"))   stop("readxl package required!")
  sh_names <- readxl::excel_sheets(path = file)
  
  sheets <- lapply(sh_names, readxl::read_xlsx, path = file, col_types = "text")
  names(sheets) <- sh_names

  return(sheets)
}


#' Writes list of data.frames as csv files into a folder
#'
#' @param sheets a list of data.frames
#' @param path path to a destination dir
#' @param ... args passed to fwrite
#' @export
worksheets_to_csvs <- function(sheets, path, ...) {
  if (!requireNamespace("data.table")) stop("data.table package required!")
  stopifnot(is.list(sheets))
  
  sh_names <- names(sheets)
  if (is.null(sh_names)) sh_names <- seq_along(sheets)
    
  invisible(
    sapply(sh_names, 
           function(s) {
             if (!is.null(sheets[[s]])) {
               data.table::fwrite(
                 x    = sheets[[s]],
                 file = file.path(path, paste0(s, ".csv")), 
                 ...)
             }
         })
  )
  
}
