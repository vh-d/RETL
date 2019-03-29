#' complete ETL pipe in a single function
#'
#' @param name name of the table
#' @param from DBI/R environment/file name
#' @param to DBI/R environment/file name
#' @param transform transformation function
#' @param name_target optional parameter specifying new name of the table at the target
#' @param from_schema optional parameter with name of schema at the origin database
#' @param to_schema optional parameter with name of schema at the target database
#' @param read_args list of arguments passed to read/extract function
#' @param write_args list of arguments passed to write/load function
#' @param asDT logical; convert the data.frame to data.table during the etl?
#' @param lowercase logical; lowercase all column names?
#'
#' @rdname pipe_table
#' @export
pipe_table <- function(
  name,
  from,
  to,
  name_target = NULL,
  transform   = NULL,
  from_schema = NULL,
  to_schema   = NULL,
  read_args   = list(),
  write_args  = list(overwrite = TRUE),
  asDT        = TRUE,
  lowercase   = FALSE
) {

  DT <- do.call(
    what = etl_read,
    args = union.list(
      list(
        from   = from, 
        name   = name, 
        schema = from_schema, 
        asDT   = asDT), 
      read_args
    )
  )
  
  # if (asDT) setDT(DT)
  if (lowercase) DT <- lowercase_names(DT)
  
  # TRANFORM PART
  if (!is.null(transform)) DT <- transform(DT)
  
  # WRITE PART
  if (!length(name_target)) name_target <- name[1]

  do.call(
    what = etl_write,
    args = union.list(
      list(
        to   = to, 
        x    = DT,
        name = name, 
        schema = to_schema), 
      write_args
    )
  )

}

#' pipe_table vectorized
#'
#' @param tables vector of table names
#' @param ... args passed to `pipe_table()`
#'
#' @rdname pipe_table
#' @export
pipe_tables <- function(tables, ...) {
  sapply(
    tables,
    function(tab) try(pipe_table(name = tab, ...))
  )
}


#' Read (extract) part of ETL
#' reading tables
#'
#' @param from DBI/R environment/file name
#' @param name character; table name
#' @param schema character;
#' @param asDT logical
#' @param lowercase logical;
#' @param ... args passed to read function
#'
#' @export
etl_read <- function(from, ...) {
  UseMethod("etl_read", from)  
}

# for all DBI-compliant drivers
etl_read.DBIConnection <- function(
  from,
  name,
  schema = NULL,
  asDT   = TRUE,
  ...
) {

  tab <- if (!is.null(schema)) paste0(schema, ".", name) else name
  
  DT <- do.call(
    DBI::dbGetQuery,
    args =
      c(list(
        conn = from,
        statement = stringr::str_interp("select * from ${tab}")),
        ...
      )
  )
  
  if (asDT) setDT(DT)
  
  return(DT)
}


# TODO: ...

#' @export
etl_read.odbc32 <- function(
  from,
  name,
  schema = NULL,
  asDT   = TRUE,
  ...
) {

  DT <- do.call(
    odbc32::sqlQuery,
    args =
        c(list(
          con   = from,
          query = stringr::str_interp("select * from ${tab}")),
          ...
        )
    )

  if (asDT) setDT(DT)
  
  return(DT)
}


#' @export
etl_read.character <- function(
  from,
  name   = NULL,
  schema = NULL,
  asDT   = TRUE,
  ...
) {
  
  DT <- do.call(
    fread,
    args = union.list(
      list(
        input = from
      ),
      list(...)
    )
  )
  
  if (!asDT) setDF(DT)
  
  return(DT)
}

#' @export
etl_read.environment <- function(
  from,
  name,
  schema = NULL,
  asDT   = TRUE,
  ...
) {
  
  DT <- copy(from[[name]])
  
  if (asDT) setDT(DT)
  
  return(DT)
}



#' Write (or load) part of ETL
#'
#' @param to DBI/R environment/file name
#' @param x data.frame object
#' @param name character;target name
#' @param schema character;
#' @param ... arguments passed to write/load function
#'
#' @export
etl_write <- function(to, ...) {
  UseMethod("etl_write", to)
}


#' @export
etl_write.DBIConnection <- function(
  to,
  x,
  name,
  schema = NULL,
  ...
) {
  tab <- if (!is.null(schema)) paste0(schema, ".", name) else name

  do.call(
    DBI::dbWriteTable,
    args = union.list(
      list(
        conn  = to,
        name  = tab,
        value = x),
      list(...)
      )
  )
  
}


#' @export
etl_write.odbc32 <- function(
  to,
  x,
  name,
  schema = NULL,
  ...
) {
  tryCatch(odbc32::sqlDrop(con = to, name = name), error = function(e) NULL)
  
  # 64bit integers cannot be passed to 32bit R session
  if (any(is_apply(x, "integer64"))) {
    x <- copy(x)
    warning("Converting 64bit integers to numeric.")
    convert_cols(x, "integer64", "numeric")
  }
  
  do.call(
    odbc32::sqlSave,
    args = union.list(
      list(
        con  = to,
        name = name,
        data = x),
      list(...)
    )
  )
}

#' @export
etl_write.character <- function(
  to,
  x,
  name   = NULL,
  schema = NULL,
  ...
) {

  do.call(
    fwrite,
    args = union.list(
      list(
        x    = x,
        file = to
      ),
      list(...)
    )
  )
  
}

#' @export
etl_write.environment <- function(
  to,
  x,
  name,
  schema = NULL,
  ...
) {
  
  to[[name]] <- x
  
  return(invisible(TRUE))
}

