# for conversion of R datatypes to a particular SQL engine's datatypes
.RODBC_DT_CONV <- 
  list(
    "ACCESS" = 
      c(
        factor    = "VARCHAR(255)",
        POSIXct   = "DATETIME",
        Date      = "DATE",
        difftime  = "TIME",
        blob      = "BINARY",
        integer   = "INTEGER",
        numeric   = "DOUBLE",
        character = "VARCHAR(255)",
        logical   = "BIT",
        list      = "VARCHAR(255)"
      )
  )
