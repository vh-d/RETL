#' derived from RCurl::merge.list()
union.list <- function (x, y, ...)
{
  if (length(x) == 0)
    return(y)
  if (length(y) == 0)
    return(x)
  i  = intersect(names(x), names(y))
  ii = setdiff(  names(y), names(x))
  # i = is.na(i)
  if (length(i))  x[i]  <- y[i]
  if (length(ii)) x[ii] <- y[ii]
  
  x
}


