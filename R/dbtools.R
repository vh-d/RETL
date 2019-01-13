#' @export
set_index <- function(table,
                      cols = c("subjekt, obdobi, rozsah, stav, mv_id, ip, radek, sloupec"),
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

#' @export
set_index_tables <- function(tables, ...){
  sapply(
    tables,
    function(tab) try(set_index(table = tab, ...))
  )
}
