
<!-- README.md is generated from README.Rmd. Please edit that file -->

# RETL

`RETL` is an R package that aims to provide practical tools for ETL
processes using R’s wide range of APIs to data sources.

It is intended to be used together with the `Rflows` package (not yet
open-sourced) as universal API to data stored in databases, files, excel
sheets. RETL relies heavily on the `data.table` package.

## Installation

RETL can be installed from [GitHub](https://github.com/vh-d/RETL) by
running:

``` r
devtools::install_github("vh-d/RETL")
```

## Examples

``` r
library(RETL)
library(magrittr)

# establish connections
my_db <- DBI::dbConnect(RSQLite::SQLite(), "path/to/my.db")
your_csv <- "path/to/your.csv"
your_db <- dbConnect(RMariaDB::MariaDB(), group = "your-db")
```

### Pipes

``` r
# simple extract and load
etl_read(from = my_db, name = "customers") %>% etl_write(to = your_csv)

# extract -> transform -> load
etl_read(from = my_db, name = "orders") %>% # db query: EXTRACT from a database
  dtq(, order_year := year(order_date)) %>% # data.table query: TRANSFORM (adding a new column)
  etl_write(to = your_db, name = "customers") # LOAD to a db
```

### Other tools

``` r
set_index(table = "customers", c("id", "order_year"), your_db)
```
