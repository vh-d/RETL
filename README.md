
<!-- README.md is generated from README.Rmd. Please edit that file -->

# RETL

`RETL` is an R package that provides tools for writing ETL jobs in R. It
stands on R’s wide range of APIs to various types of data sources.

It is intended to be used together with the
*[Rflow](https://github.com/vh-d/Rflow)* and
*[RETLflow](https://github.com/vh-d/RETLflow)* packages as universal API
to data stored in databases, files, excel sheets. RETL relies heavily on
the `data.table` package for fast data transofrmations.

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
my_db    <- DBI::dbConnect(RSQLite::SQLite(), "path/to/my.db")
your_csv <- "path/to/your.csv"
your_db  <- dbConnect(RMariaDB::MariaDB(), group = "your-db")
```

### Pipes

``` r
# simple extract and load
etl_read(from = your_csv) %>% etl_write(to = my_db, name = "customers")

# extract -> transform -> load
etl_read(from = my_db, name = "orders") %>% # extract from a database
  dbq(, order_year := year(order_date)) %>% # transform (adding a new column)
  etl_write(to = your_db, name = "customers") # load
```

### Other tools

``` r
set_index(table = "customers", c("id", "order_year"), your_db)
```
