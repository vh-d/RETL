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
  lowercase   = TRUE
) {

  tab <- if (!is.null(from_schema)) paste0(from_schema, ".", name) else name

  if (tryCatch(DBI::dbIsValid(from), error = function(e) FALSE) == TRUE) {

    DT <- do.call(
      dbGetQuery,
      args =
        c(list(
          conn = from,
          statement = stringr::str_interp("select * from ${tab}")),
          read_args
        )
    )

  } else if (is(from, "odbc32")) {

    DT <- do.call(
      odbc32::sqlQuery,
      args =
        c(list(
          con   = from,
          query = stringr::str_interp("select * from ${tab}")),
          read_args
        )
    )

  } else if (is(from, "character") && grepl(".csv", from, ignore.case = TRUE)) {

    DT <- do.call(
      read.table,
      args =
        c(list(
          file  = from
        ),
        read_args
        )
    )

  } else if (is(from, "environment")) {

    DT <- from[[tab]]

  } else {
    stop("'from' is neither an environemnt nor a DBI connection!")
  }

  if (asDT) setDT(DT)
  if (lowercase) DT <- lowercase(DT)

  # for SQLite convert dates as character
  if (is(to, "SQLiteConnection")) Date2Char(DT)


  # TRANFORM PART
  if (!is.null(transform)) DT <- transform(DT)


  # WRITE PART
  if (!length(name_target)) name_target <- name[1]
  tab <- if (!is.null(to_schema)) paste0(to_schema, ".", name[1]) else name[1]

  if (tryCatch(DBI::dbIsValid(to), error = function(e) FALSE) == TRUE) {

    do.call(
      dbWriteTable,
      args = c(list(
        conn  = to,
        name  = tab,
        value = DT),
        write_args))

  } else if (is(to, "odbc32")) {

    tryCatch(odbc32::sqlDrop(con = to, name = tab), error = function(e) NULL)

    # to avoid sending integer64 to 32-bit excel convert all to numeric
    int64_to_numeric(DT)

    do.call(
      odbc32::sqlSave,
      args = c(list(
        con  = to,
        name = tab,
        data = DT),
        write_args))

  } else if (is(to, "environment")) {
    setDT(DT)
    to[[tab]] <- DT

    return(invisible(TRUE))

  } else {
    stop("'to' is neither an environemnt not a DBI connection!")
  }
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
etl_read <- function(
  from,
  name,
  schema = NULL,
  asDT       = TRUE,
  lowercase   = TRUE,
  ...
) {

  tab <- if (!is.null(schema)) paste0(schema, ".", name) else name

  if (tryCatch(DBI::dbIsValid(from), error = function(e) FALSE) == TRUE) {

    DT <- do.call(
      dbGetQuery,
      args =
        c(list(
          conn = from,
          statement = stringr::str_interp("select * from ${tab}")),
          ...
        )
    )

  } else if (is(from, "odbc32")) {

    DT <- do.call(
      odbc32::sqlQuery,
      args =
        c(list(
          con   = from,
          query = stringr::str_interp("select * from ${tab}")),
          ...
        )
    )

  } else if (is(from, "character") && grepl(".csv", from, ignore.case = TRUE)) {

    DT <- do.call(
      read.table,
      args =
        c(list(
          file  = from
        ),
        ...
        )
    )

  } else if (is(from, "environment")) {

    DT <- from[[tab]]

  } else {
    stop("'from' is neither an environemnt nor a DBI connection!")
  }

  if (asDT) setDT(DT)
  if (lowercase) DT <- lowercase_names(DT)

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
etl_write <- function(
  to,
  x,
  name,
  schema = NULL,
  ...
) {

  tab <- if (!is.null(schema)) paste0(schema, ".", name) else name

  if (tryCatch(DBI::dbIsValid(to), error = function(e) FALSE) == TRUE) {

    do.call(
      dbWriteTable,
      args = c(list(
        conn  = to,
        name  = tab,
        value = x),
        ...))

  } else if (is(to, "odbc32")) {

    tryCatch(odbc32::sqlDrop(con = to, name = tab), error = function(e) NULL)

    do.call(
      odbc32::sqlSave,
      args = c(list(
        con  = to,
        name = tab,
        data = x),
        ...))

  } else if (is(to, "environment")) {
    setDT(x)
    to[[tab]] <- x

    return(invisible(TRUE))

  } else {
    stop("'to' object not recognized! Supported classes are DBI connection, odbc32 connection, R environment, character (csv file name)")
  }
}

