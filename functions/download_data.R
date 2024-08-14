#' Download data from multiple sources
#' 
#' This function is a wrapper to download data from three sources:
#' GLORYS, Coraltemp and MUR SST.
#'
#' @param temp_source description
#' @param obis_sel_month OBIS data prepared with workID (subset grid)
#' @param sel_month selected month for download
#' @param sel_year selected year for download
#' @param outfolder output folder for the environmental data files
#' @param ... other named parameters. For GLORYS, `dataset` (code for the dataset)
#'   and `variables` (code for variables).
#'
#' @return list of files
#' @export
#'
#' @examples
#' \dontrun{
#' 
#' }
download_temp <- function(temp_source,
                          obis_sel_month,
                          sel_month,
                          sel_year,
                          outfolder,
                          ...) {
  
  arguments <- list(
    obis_sel_month = obis_sel_month,
    sel_month = sel_month,
    sel_year = sel_year,
    outfolder = outfolder,
    ...
  )
  
  download_res <- switch(
    temp_source,
    glorys = rlang::exec(.download_glorys, !!!arguments),
    coraltemp = rlang::exec(.download_coraltemp, !!!arguments),
    mur = rlang::exec(.download_mur, !!!arguments)
  )
  
  return(download_res)
  
}

#' Download data from Copernicus GLORYS
#'
#' @param obis_sel_month OBIS data prepared with workID (subset grid)
#' @param sel_month selected month for download
#' @param sel_year selected year for download
#' @param outfolder output folder for the environmental data files
#' @param dataset Copernicus dataset
#' @param variables variables of the dataset
#'
#' @return list of files
#' @export
#'
#' @examples
#' \dontrun{
#' 
#' }
.download_glorys <- function(obis_sel_month, sel_month, sel_year, outfolder,
                             dataset, variables) {
  
  outfile_list <- lapply(unique(obis_sel_month$workID), function(id){
    # Get longitude and latitude range and add a buffer
    long_range <- range(obis_sel_month$decimalLongitude[obis_sel_month$workID == id])
    long_range <- long_range + c(-2, 2)
    
    lat_range <- range(obis_sel_month$decimalLatitude[obis_sel_month$workID == id])
    lat_range <- lat_range + c(-2, 2)
    
    # Check if lon/lat are valid
    if (long_range[1] < -180) long_range[1] <- -180
    if (long_range[2] > 180) long_range[2] <- 180
    if (lat_range[1] < -90) lat_range[1] <- -90
    if (lat_range[2] > 90) lat_range[2] <- 90
    
    # Define outfile for the temp file
    outfile <- paste0("temp_", sel_year, "_", sel_month, "_", id)
    
    # Download data
    cm$subset(
      dataset_id = dataset,
      variables = variables,
      username = .user,
      password = .pwd,
      minimum_longitude = long_range[1],
      maximum_longitude = long_range[2],
      minimum_latitude = lat_range[1],
      maximum_latitude = lat_range[2],
      start_datetime = paste0(sel_year, "-", sprintf("%02d", sel_month), "-01T00:00:00"),
      end_datetime = paste0(sel_year, "-", sprintf("%02d", sel_month), "-",
                            lubridate::days_in_month(lubridate::as_date(paste(sel_year, sel_month, sep = "-"), format = "%Y-%m"))
                            ,"T23:59:59"),
      # minimum_depth = min(depths$depth),
      # maximum_depth = max(depths$depth),
      output_filename = outfile,
      output_directory = outfolder,
      force_download = TRUE
    )
    
    return(outfile)
    
  })
  
  return(outfile_list)
  
}

#' Download data from NOAA Coraltemp
#'
#' @param obis_sel_month OBIS data prepared with workID (subset grid)
#' @param sel_month selected month for download
#' @param sel_year selected year for download
#' @param outfolder output folder for the environmental data files
#'
#' @return list of files
#' @export
#'
#' @examples
#' \dontrun{
#' 
#' }
.download_coraltemp <- function(obis_sel_month, sel_month, sel_year, outfolder) {
  
  outfile_list <- lapply(unique(obis_sel_month$workID), function(id){
    # Get longitude and latitude range and add a buffer
    long_range <- range(obis_sel_month$decimalLongitude[obis_sel_month$workID == id])
    long_range <- long_range + c(-2, 2)
    
    lat_range <- range(obis_sel_month$decimalLatitude[obis_sel_month$workID == id])
    lat_range <- lat_range + c(-2, 2)
    
    # Check if lon/lat are valid
    if (long_range[1] < -179.975) long_range[1] <- -179.975
    if (long_range[2] > 179.975) long_range[2] <- 179.975
    if (lat_range[1] < -89.975) lat_range[1] <- -89.975
    if (lat_range[2] > 89.975) lat_range[2] <- 89.975
    
    # Define outfile for the temp file
    outfile <- paste0("temp_", sel_year, "_", sel_month, "_", id, "_ct")
    
    # Download data
    rerddap::griddap(
      "NOAA_DHW_monthly",
      time = rep(paste0(sel_year, "-", sprintf("%02d", sel_month), "-16T00:00:00Z"), 2),
      latitude = lat_range,
      longitude = long_range,
      fields = "sea_surface_temperature",
      fmt = "nc",
      url = "https://coastwatch.pfeg.noaa.gov/erddap",
      store = rerddap::disk(paste0(outfolder, "/", outfile)),
      read = FALSE
    )
    
    return(outfile)
    
  })
  
  return(outfile_list)
  
}

#' Download data from NASA MUR
#'
#' @param obis_sel_month OBIS data prepared with workID (subset grid)
#' @param sel_month selected month for download
#' @param sel_year selected year for download
#' @param outfolder output folder for the environmental data files
#'
#' @return list of files
#' @export
#'
#' @examples
#' \dontrun{
#' 
#' }
.download_mur <- function(obis_sel_month, sel_month, sel_year, outfolder) {
  
  outfile_list <- lapply(unique(obis_sel_month$workID), function(id){
    # Get longitude and latitude range and add a buffer
    long_range <- range(obis_sel_month$decimalLongitude[obis_sel_month$workID == id])
    long_range <- long_range + c(-2, 2)
    
    lat_range <- range(obis_sel_month$decimalLatitude[obis_sel_month$workID == id])
    lat_range <- lat_range + c(-2, 2)
    
    # Check if lon/lat are valid
    if (long_range[1] < -179.99) long_range[1] <- -179.99
    if (long_range[2] > 180) long_range[2] <- 180
    if (lat_range[1] < -89.99) lat_range[1] <- -89.99
    if (lat_range[2] > 89.99) lat_range[2] <- 89.99
    
    # Define outfile for the temp file
    outfile <- paste0("temp_", sel_year, "_", sel_month, "_", id, "_mur")
    
    # Download data
    rerddap::griddap(
      "jplMURSST41mday",
      time = rep(paste0(sel_year, "-", sprintf("%02d", sel_month), "-16T00:00:00Z"), 2),
      latitude = lat_range,
      longitude = long_range,
      fields = "sst",
      fmt = "nc",
      url = "https://coastwatch.pfeg.noaa.gov/erddap",
      store = rerddap::disk(paste0(outfolder, "/", outfile)),
      read = FALSE
    )
    
    return(outfile)
    
  })
  
  return(outfile_list)
  
}


#' Load and mosaic layers
#'
#' @param outfolder folder where temporary files are stored
#' @param files_list list of files returned by [download_data()]
#'
#' @return terra SpatRaster
#' @export
#'
#' @examples
#' \dontrun{
#' 
#' }
load_layers <- function(outfolder, files_list) {
  if (file_test("-d", paste0(outfolder, "/", files_list[[1]]))) {
    files_list <- lapply(files_list, function(x) {
      paste0(outfolder, "/", x, "/", list.files(paste0(outfolder, "/", x)))
    })
  } else {
    files_list <- paste0(outfolder, "/", unlist(files_list), ".nc")
  }
  ind_layers <- lapply(files_list, terra::rast)
  if (length(ind_layers) > 1) {
    layer <- terra::merge(terra::sprc(ind_layers))
  } else {
    layer <- terra::rast(ind_layers)
  }
  return(layer)
}