#' Download data from multiple sources
#' 
#' This function is a wrapper to download data from three sources:
#' GLORYS, Coraltemp and MUR SST.
#'
#' @param temp_source name of the source. One of 'glorys', 'coraltemp', 'mur' or 'ostia'
#' @param dataset dataset code for GLORYS and OSTIA or dataset address for CoralTemp and MUR
#' @param obis_dataset OBIS data prepared
#' @param sel_month selected month for download
#' @param sel_year selected year for download
#'
#' @return list of files
#' @export
#'
#' @examples
#' \dontrun{
#' 
#' }
download_temp <- function(temp_source,
                          dataset,
                          obis_dataset,
                          sel_month,
                          sel_year) {

  xr <- reticulate::import("xarray")
  cm <- import("copernicusmarine")

  if (Sys.getenv("COPERNICUS_USER") != "") {
    .user <- Sys.getenv("COPERNICUS_USER")
    .pwd <- Sys.getenv("COPERNICUS_PWD")
  } else {
    .user <- rstudioapi::askForPassword("Enter your user")
    .pwd <- rstudioapi::askForPassword("Enter your password")
  }

  reticulate::source_python("functions/download_glorys_py.py")
  reticulate::source_python("functions/download_coraltemp_py.py")
  reticulate::source_python("functions/download_mur_py.py")
  reticulate::source_python("functions/download_ostia_py.py")

  if (inherits(dataset, "xarray.core.dataset.Dataset")) {
    ds <- dataset
  } else {
    ds <- switch(
    temp_source,
    glorys = cm$open_dataset(
      dataset_id = dataset,
      username = .user,
      password = .pwd),
    coraltemp = xr$open_dataset(dataset),
    mur = xr$open_dataset(dataset),
    ostia = cm$open_dataset(
      dataset_id = dataset,
      username = .user,
      password = .pwd)
  )
  }

  ds <- ds$chunk("auto")

  sel_date <- paste0(sel_year, "-", sprintf("%02d", sel_month), "-01")
  
  arguments <- list(
    dataset = ds,
    target_data = obis_dataset,
    sel_date = sel_date
  )
  
  download_res <- switch(
    temp_source,
    glorys = rlang::exec(download_glorys_py, !!!arguments),
    coraltemp = rlang::exec(download_coraltemp_py, !!!arguments),
    mur = rlang::exec(download_mur_py, !!!arguments),
    ostia = rlang::exec(download_ostia_py, !!!arguments)
  )
  
  return(download_res)
  
}
