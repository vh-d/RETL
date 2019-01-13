#' @param table character;name of the table
#'
#' @param cols character; vector of columns used for creating an index
#' @param conn DBI connection
#'
#' @rdname set_index
#' @export
set_index <- function(table,
                      cols,
                      conn) {
  if (try(DBI::dbIsValid(conn)) == TRUE) {
    dbExecute(
      conn = conn,
      statement = str_interp("CREATE INDEX idx_${table} ON ${table} (${cols})")
    )
  } else {
    setkeyv(x = conn[[table]], cols = strsplit(cols, ",\\s*")[[1]])
    return(TRUE)
  }
}

#' @rdname set_index
#' @export
set_index_tables <- function(tables, ...){
  sapply(
    tables,
    function(tab) try(set_index(table = tab, ...))
  )
}
