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


#' complete ETL pipe
#'
#' @param from DBI/R environment/file name/...
#' @param to DBI/R environment/file name/...
#' @param transform transformation function
#' @param read_args list of arguments passed to read/extract function
#' @param write_args list of arguments passed to write/load function
#' @param asDT logical; convert the data.frame to data.table during the etl?
#'
#' @rdname pipe_table
#' @export
etl_pipe <- function(
  from,
  to,
  transform   = NULL,
  read_args   = list(),
  write_args  = list(overwrite = TRUE),
  asDT        = TRUE,
  lowercase   = FALSE
) {
  
  # EXTRACT/READ
  DT <- do.call(
    what = etl_read,
    args = union.list(
      list(
        from = from, 
        asDT = asDT
      ), 
      read_args
    )
  )
  
  # TRANFORM
  if (!is.null(transform)) DT <- transform(DT)
  
  # WRITE
  do.call(
    what = etl_write,
    args = union.list(
      list(
        to = to, 
        x  = DT
      ), 
      write_args
    )
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
#' @export
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
  
  if (length(schema)) name <- paste0(schema, ".", name)
  
  DT <- do.call(
    odbc32::sqlQuery,
    args =
      c(list(
        con   = from,
        query = stringr::str_interp("select * from ${name}")),
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
#' @seealso
#' \code{\link{etl_write.DBIConnection}} for writing over DBI driver, 
#' 
#' \code{\link{etl_write.character}} for writing to CSV files, 
#' 
#' \code{\link{etl_write.odbc32}} for writing over odbc32 driver
#' 
#' \code{\link{etl_write.environment}} for writing to R environment
etl_write <- function(to, ...) {
  UseMethod("etl_write", to)
}


#' Write tables via DBI connection
#' @param to 
#' @param x data.table
#' @param name name of target table
#' @param schema prefix to table name
#' @param ... args passed to dbWriteTable
#'
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



#' Write tables via RODBC connection
#' @param to 
#'
#' @param x data.table
#' @param name name of the target table
#' @param schema ignored
#' @param rownames logical;
#' @param safer logical;
#' @param varTypes names character vector
#' @param ... 
#'
#' @export
etl_write.RODBC <- function(
  to,
  x,
  name,
  schema = NULL, # ignored?
  rownames = FALSE,
  safer = FALSE,
  varTypes = NULL,
  ...
) {
  tryCatch(RODBC::sqlDrop(channel = to, sqtable = name, errors = TRUE), error = function(e) NULL)
  
  # 64bit integers cannot be passed to 32bit R session
  if (any(is_apply(x, "integer64"))) {
    x <- copy(x)
    warning("Converting 64bit integers to numeric.")
    convert_cols(x, "integer64", "numeric")
  }
  
  # resolve datatypes
  if (!length(varTypes)) {
    driver <- RODBC::odbcGetInfo(to)[[1L]]
    varTypes <- .RODBC_DT_CONV[[driver]][sapply(x, function(y) tail(class(y), 1L))]
    names(varTypes) <- names(x)
  }
  
  RODBC::sqlSave(
    channel = to, 
    tablename = name, 
    dat = x, 
    rownames = rownames, 
    safer = safer,
    varTypes = varTypes,
    ...
  )

}

#' Write tables via odbc32 connection
#' @param to 
#'
#' @param x data.table
#' @param name name of the target table
#' @param schema ignored
#' @param rownames logical;
#' @param safer logical;
#' @param varTypes names character vector
#' @param ... 
#'
#' @export
etl_write.odbc32 <- function(
  to,
  x,
  name,
  schema = NULL, # ignored?
  rownames = FALSE,
  safer = FALSE,
  varTypes = NULL,
  ...
) {
  tryCatch(odbc32::sqlDrop(con = to, name = name), error = function(e) NULL)
  
  # 64bit integers cannot be passed to 32bit R session
  if (any(is_apply(x, "integer64"))) {
    x <- copy(x)
    warning("Converting 64bit integers to numeric.")
    convert_cols(x, "integer64", "numeric")
  }
  
  # resolve datatypes
  if (!length(varTypes)) {
    driver <- odbc32::odbcGetInfo(to)[[1L]]
    varTypes <- .RODBC_DT_CONV[[driver]][sapply(x, function(y) tail(class(y), 1L))]
    names(varTypes) <- names(x)
  }
  
  do.call(
    odbc32::sqlSave,
    args = union.list(
      list(
        con  = to,
        name = name,
        data = x,
        rownames = rownames,
        safer = safer,
        varTypes = varTypes
      ),
      list(...)
    )
  )
}

#' Write tables to CSV files
#' @param to name of the target file
#' @param x data.table
#' @param name ignored
#' @param schema ignored
#' @param ... args passed to `fwrite()`
#'
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

#' Write tables to R environment
#' @param to R environemnt
#' @param x data.table
#' @param name name of the target object
#' @param ... ignored
#' @export
etl_write.environment <- function(
  to,
  x,
  name,
  ...
) {
  
  to[[name]] <- x
  
  return(invisible(TRUE))
}

