# Create a function to get and one to check date
get_date <- function(x) {
  as.POSIXct(x / 1000, origin = "1970-01-01")
}

check_date <- function(start, end) {
  start_d <- lubridate::as_date(paste0(
    lubridate::year(start),
    "-", lubridate::month(start)
  ), format = "%Y-%m")
  
  end_d <- lubridate::as_date(paste0(
    lubridate::year(end),
    "-", lubridate::month(end)
  ), format = "%Y-%m")
  
  difference <- end_d - start_d
  
  flag <- ifelse(difference != 0, 1, 0)
  
  return(flag)
}