# for conversion of R datatypes to a particular SQL engine's datatypes
.RODBC_DT_CONV <- new.env()

.RODBC_DT_CONV[["ACCESS"]] <-  
  c(
    factor    = "VARCHAR(255)",
    POSIXt    = "DATETIME",
    Date      = "DATE",
    difftime  = "TIME",
    blob      = "BINARY",
    integer   = "INTEGER",
    numeric   = "DOUBLE",
    character = "VARCHAR(255)",
    logical   = "VARCHAR(5)",
    list      = "VARCHAR(255)"
  )


# TODO: use S3 methods a la odbc package
