######################### OBIS sea temperature dataset ########################
# January of 2024
# Author: Silas C. Principe
# Contact: s.principe@unesco.org
#
################################# Data download ################################

# Flags
# 0 = no problem
# 1 = date is range
# 2 = surfaceTemperature is approximated
# 4 = midTemperature is approximated
# 8 = deepTemperature is approximated
# 16 = minimumDepthTemperature is approximated
# 32 = maximumDepthTemperature is approximated
# 64 = bottomTemperature is approximated
# 128 = coraltempSST is approximated
# 256 = murSST is approximated
# 512 = ostiaSST is approximated
# Flags can be summed. 
# E.g. a flag = 6 means that date is range and surfaceTemperature and medianTemperature are approximated

# Load packages ----
library(terra)
library(reticulate)
library(arrow)
library(parallel)
library(dplyr)
library(tidyr)
cm <- import("copernicusmarine")
source("functions/nearby_extract.R")
source("functions/dates.R")
source("functions/subset_grid.R")
source("functions/download_data.R")
source("functions/utils.R")

# Settings ----
if (Sys.getenv("COPERNICUS_USER") != "") {
  .user <- Sys.getenv("COPERNICUS_USER")
  .pwd <- Sys.getenv("COPERNICUS_PWD")
} else {
  .user <- rstudioapi::askForPassword("Enter your user")
  .pwd <- rstudioapi::askForPassword("Enter your password")
}

outfolder <- "temp"
outfolder_final <- "results"
fs::dir_create(outfolder)
fs::dir_create(outfolder_final)
filename <- "var=thetao"
coordnames <- c("decimalLongitude", "decimalLatitude")

# Define target dataset for Copernicus, time and depths
dataset <- "cmems_mod_glo_phy_my_0.083deg_P1M-m"
product <- "glorys" # This is also used to name the files, as the "core" product
variables <- list("thetao")

ds_sample <- cm$open_dataset(
  dataset_id = dataset,
  #variables = variables,
  username = .user,
  password = .pwd,
  minimum_longitude = -10,
  maximum_longitude = 10,
  minimum_latitude = -10,
  maximum_latitude = 10
)

# Print loaded dataset information
print(ds_sample)

# Get available depths and maximum date of first dataset
depths <- ds_sample$depth$to_dataframe()
glorys1_max_date <- max(ds_sample$time$to_dataframe()[,1])

# Define range of dates to get information
range_year <- 2023
range_month <- 1:12

# Open OBIS dataset
obis_ds <- open_dataset(get_obis())

# Limit by selected columns
obis_filt <- obis_ds %>%
  select(AphiaID, species, family,
         id, dataset_id, occurrenceID, datasetID,
         minimumDepthInMeters, maximumDepthInMeters,
         decimalLongitude, decimalLatitude,
         eventDate, date_start, date_mid, date_end) %>%
  filter(!is.na(eventDate))

# Define ranges that are available per product
glorys_range <- 1993:lubridate::year(Sys.Date())
coraltemp_range <- 1986:lubridate::year(Sys.Date())
mur_range <- 2002:lubridate::year(Sys.Date())
ostia_range <- 2007:lubridate::year(Sys.Date())

log_df <- data.frame(year = rep(range_year, each = 12), 
                     month = rep(range_month, length(range_year)),
                     status_glorys = NA,
                     status_coraltemp = NA,
                     status_mur = NA,
                     status_ostia = NA,
                     status_general = NA)

for (yr in seq_along(range_year)) {
  
  sel_year <- range_year[yr]
  cat("Downloading data for year", sel_year, "\n")
  
  # Load data for a specific year and month
  eval(parse(text = paste0(
    "obis_sel <- obis_filt %>%
        filter(grepl('", sel_year, "', eventDate)) %>%
        collect()"
  )))
  
  if (nrow(obis_sel) > 0) {
    obis_sel <- obis_sel %>%
      mutate(extractedDateStart = get_date(date_start),
             extractedDateMid = get_date(date_mid),
             extractedDateEnd = get_date(date_end)) %>%
      filter(!is.na(extractedDateMid)) %>%
      mutate(extractedDateYear = lubridate::year(extractedDateMid),
             extractedDateMonth = lubridate::month(extractedDateMid)) %>%
      mutate(flagDate = check_date(extractedDateStart, extractedDateEnd))
  }
  
  for (mo in seq_along(range_month)) {
    
    sel_month <- range_month[mo]
    cat("Proccessing month", sel_month, "\n")
    
    if (nrow(obis_sel) > 0) {
      obis_sel_month <- obis_sel %>%
        filter(extractedDateYear == sel_year)  %>%
        filter(extractedDateMonth == sel_month) %>%
        mutate(temp_ID = seq_len(nrow(.)))
      
      obis_sel_month <- subset_grid(obis_sel_month)
    } else {
      obis_sel_month <- obis_sel
    }
    
    if (nrow(obis_sel_month) > 0) {
      
      all_vals <- data.frame(
        temp_ID = obis_sel_month$temp_ID,
        surfaceTemperature = NA,
        midTemperature = NA,
        deepTemperature = NA,
        bottomTemperature = NA,
        midDepth = NA,
        deepDepth = NA,
        minimumDepthTemperature = NA,
        maximumDepthTemperature = NA,
        minimumDepthClosestDepth = NA,
        maximumDepthClosestDepth = NA,
        coraltempSST = NA,
        murSST = NA,
        ostiaSST = NA,
        flag = obis_sel_month$flagDate
      )
      
      # GLORYS PRODUCT ----
      if (sel_year %in% glorys_range) {

        if (as.Date(paste0(sel_year, "-", sprintf("%02d", sel_month), "-01")) <= glorys1_max_date) {
          dataset <- "cmems_mod_glo_phy_my_0.083deg_P1M-m"
        } else {
          dataset <- "cmems_mod_glo_phy_myint_0.083deg_P1M-m"
        }
        
        success <- try(download_temp("glorys", obis_sel_month, sel_month, sel_year,
                                     outfolder, dataset = dataset, 
                                     variables = variables))
        
        if (!inherits(success, "try-error")) {
          
          layer <- load_layers(outfolder, success)
          
          max_dep <- terra::app(layer, fun = function(x) {
            if (any(!is.na(x))) {
              max(which(!is.na(x)))
            } else {
              NA
            }
          })
          
          # Surface values
          surface_vals <- terra::extract(layer[[1]], obis_sel_month[,coordnames], ID = F)
          
          na_to_solve <- which(is.na(surface_vals[,1]))
          
          near_surface_vals <- get_nearby(obis_sel_month[na_to_solve, coordnames],
                                          layer[[1]], mode = "24")
          
          surface_vals[na_to_solve,] <- near_surface_vals
          all_vals$flag[na_to_solve][!is.na(surface_vals[na_to_solve,])] <-
            all_vals$flag[na_to_solve][!is.na(surface_vals[na_to_solve,])] + 2
          
          na_to_remove <- which(is.na(surface_vals[,1]))
          
          # Mid depth values
          mid_dep <- max_dep
          mid_dep[!is.na(mid_dep)] <- 1
          mid_dep <- c(mid_dep, max_dep)
          mid_dep <- floor(median(mid_dep))
          
          which_mid <- terra::extract(mid_dep, obis_sel_month[,coordnames], ID = F)
          
          na_to_solve <- which(is.na(which_mid[,1]))
          
          which_mid[na_to_solve,] <- get_nearby(obis_sel_month[na_to_solve, coordnames],
                                                    mid_dep, mode = "24",
                                                    type = "median")
          
          which_mid[na_to_remove,1] <- 1
          
          mid_vals <- terra::extract(layer, obis_sel_month[,coordnames],
                                     layer = which_mid[,1], ID = F)
          
          mid_vals[na_to_solve, 2] <- get_nearby_mlayer(obis_sel_month[na_to_solve, coordnames],
                                                  layer, mode = "24",
                                                  type = "mean",
                                                  layer = which_mid[na_to_solve,1])
          
          mid_vals[na_to_remove,] <- NA
          all_vals$flag[na_to_solve][!is.na(mid_vals[na_to_solve,2])] <-
            all_vals$flag[na_to_solve][!is.na(mid_vals[na_to_solve,2])] + 4
          
          # Maximum depth values
          which_max <- terra::extract(max_dep, obis_sel_month[,coordnames], ID = F)
          
          na_to_solve <- which(is.na(which_max[,1]))
          
          which_max[na_to_solve,] <- get_nearby(obis_sel_month[na_to_solve, coordnames],
                                                max_dep, mode = "24",
                                                type = "median")
          
          which_max[na_to_remove,1] <- 1
          
          max_vals <- terra::extract(layer, obis_sel_month[,coordnames],
                                     layer = which_max[,1], ID = F)
          
          max_vals[na_to_solve,2] <- get_nearby_mlayer(obis_sel_month[na_to_solve, coordnames],
                                                               layer, mode = "24",
                                                               type = "mean",
                                                               layer = which_max[na_to_solve,1])
          
          max_vals[na_to_remove,] <- NA
          all_vals$flag[na_to_solve][!is.na(max_vals[na_to_solve,2])] <-
            all_vals$flag[na_to_solve][!is.na(max_vals[na_to_solve,2])] + 8
          
          # Aggregate info
          all_vals$surfaceTemperature <- surface_vals[,1]
          all_vals$midTemperature <- mid_vals[,2]
          all_vals$deepTemperature <- max_vals[,2]
          all_vals$midDepth <- as.numeric(gsub(".*.=", "", mid_vals[,1]))
          all_vals$deepDepth <- as.numeric(gsub(".*.=", "", max_vals[,1]))
          
          if (any(!is.na(obis_sel_month$minimumDepthInMeters)) || any(!is.na(obis_sel_month$maximumDepthInMeters))) {
            which_have_min <- which(!is.na(obis_sel_month$minimumDepthInMeters))
            which_have_max <- which(!is.na(obis_sel_month$maximumDepthInMeters))
            
            if (length(which_have_min) > 0) {
              min_obis <- obis_sel_month$minimumDepthInMeters[which_have_min]
              
              closest_min <- sapply(min_obis, function(x) {
                cval <- which.min(abs(depths$depth - x))
                return(cval[1])
              })
              
              min_obis_vals <- terra::extract(layer, obis_sel_month[which_have_min, coordnames],
                                              layer = closest_min, ID = F)
              
              if (any(is.na(min_obis_vals[,2]))) {
                na_to_solve <- which(is.na(min_obis_vals[,2]))
                
                min_obis_vals[na_to_solve, 2] <- get_nearby_mlayer(obis_sel_month[which_have_min[na_to_solve], 
                                                                                    coordnames],
                                                                     layer, mode = "24",
                                                                     type = "mean",
                                                                     layer = closest_min[na_to_solve])
                
                all_vals$flag[which_have_min[na_to_solve]] <- all_vals$flag[which_have_min[na_to_solve]] + 16
            }
              
              all_vals$minimumDepthTemperature[which_have_min] <- min_obis_vals[,2]
              all_vals$minimumDepthClosestDepth[which_have_min] <- as.numeric(gsub(".*.=", "", min_obis_vals[,1]))
              
            }
            
            if (length(which_have_max) > 0) {
              max_obis <- obis_sel_month$maximumDepthInMeters[which_have_max]
              
              closest_max <- sapply(max_obis, function(x) {
                cval <- which.min(abs(depths$depth - x))
                return(cval[1])
              })
              
              max_obis_vals <- terra::extract(layer, obis_sel_month[which_have_max, coordnames],
                                              layer = closest_max, ID = F)
              
              if (any(is.na(max_obis_vals[,2]))) {
                na_to_solve <- which(is.na(max_obis_vals[,2]))
                
                max_obis_vals[na_to_solve, 2] <- get_nearby_mlayer(obis_sel_month[which_have_max[na_to_solve], 
                                                                                  coordnames],
                                                                   layer, mode = "24",
                                                                   type = "mean",
                                                                   layer = closest_max[na_to_solve])
                
                all_vals$flag[which_have_max[na_to_solve]] <- all_vals$flag[which_have_max[na_to_solve]] + 32
              }
              
              all_vals$maximumDepthTemperature[which_have_max] <- max_obis_vals[,2]
              all_vals$maximumDepthClosestDepth[which_have_max] <- as.numeric(gsub(".*.=", "", max_obis_vals[,1]))
              
            }
            
          }
          
          rm(success, layer)
          fs::file_delete(list.files(outfolder, full.names = T))

          success <- try(download_temp("glorys", obis_sel_month, sel_month, sel_year,
                                     outfolder, dataset = dataset, 
                                     variables = list("bottomT")))
        
          if (!inherits(success, "try-error")) {
            layer <- load_layers(outfolder, success)

            bottom_vals <- terra::extract(layer[[1]], obis_sel_month[,coordnames], ID = F)
          
            na_to_solve <- which(is.na(bottom_vals[,1]))
          
            near_bottom_vals <- get_nearby(obis_sel_month[na_to_solve, coordnames],
                                          layer[[1]], mode = "24")
          
            bottom_vals[na_to_solve,] <- near_bottom_vals
            all_vals$flag[na_to_solve][!is.na(bottom_vals[na_to_solve,])] <-
              all_vals$flag[na_to_solve][!is.na(bottom_vals[na_to_solve,])] + 64
            
            all_vals$bottomTemperature <- bottom_vals[,1]

          }
          fs::file_delete(list.files(outfolder, full.names = T))

        }
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_glorys"] <- "concluded"
      } else {
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_glorys"] <- "unavailable"
      }
      
      # CORALTEMP PRODUCT ----
      if (sel_year %in% coraltemp_range) {
        
        cat("Downloading CoralTemp\n")
        
        success <- try(download_temp("coraltemp", obis_sel_month, sel_month, sel_year,
                                     outfolder))
        
        if (!inherits(success, "try-error")) {
          
          cat("Proccessing CoralTemp\n")
          
          layer <- load_layers(outfolder, success)
          
          surface_ct_vals <- terra::extract(layer, obis_sel_month[,coordnames], ID = F)
          
          na_to_solve <- which(is.na(surface_ct_vals[,1]))
          
          surface_ct_vals[na_to_solve, 1] <- get_nearby(obis_sel_month[na_to_solve, coordnames],
                                                        layer, mode = "24",
                                                        type = "mean")
          
          all_vals$flag[na_to_solve[!is.na(surface_ct_vals[na_to_solve, 1])]] <- 
            all_vals$flag[na_to_solve[!is.na(surface_ct_vals[na_to_solve, 1])]] + 128
          
          all_vals$coraltempSST <- surface_ct_vals[,1]
          
        }

        fs::file_delete(list.files(outfolder, full.names = T))
        
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_coraltemp"] <- "concluded"
      } else {
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_coraltemp"] <- "unavailable"
      }
      
      
      # MUR PRODUCT ----
      if (sel_year %in% mur_range) {
        
        cat("Downloading MUR\n")
        
        success <- try(download_temp("mur", obis_sel_month, sel_month, sel_year,
                                     outfolder))
        
        if (!inherits(success, "try-error")) {
          
          cat("Proccessing MUR\n")
          
          layer <- load_layers(outfolder, success)
          
          surface_mt_vals <- terra::extract(layer, obis_sel_month[,coordnames], ID = F)
          
          na_to_solve <- which(is.na(surface_mt_vals[,1]))
          
          surface_mt_vals[na_to_solve, 1] <- get_nearby(obis_sel_month[na_to_solve, coordnames],
                                                             layer, mode = "24",
                                                             type = "mean")
          
          all_vals$flag[na_to_solve[!is.na(surface_mt_vals[na_to_solve, 1])]] <- 
            all_vals$flag[na_to_solve[!is.na(surface_mt_vals[na_to_solve, 1])]] + 256
          
          all_vals$murSST <- surface_mt_vals[,1]
          
        }

        fs::file_delete(list.files(outfolder, full.names = T))
        
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_mur"] <- "concluded"
      } else {
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_mur"] <- "unavailable"
      }


      # OSTIA PRODUCT ----
      if (sel_year %in% ostia_range) {
        
        cat("Downloading OSTIA\n")
        
        success <- try(download_temp("ostia", obis_sel_month, sel_month, sel_year,
                                     outfolder))
        
        if (!inherits(success, "try-error")) {
          
          cat("Proccessing OSTIA\n")
          
          layer <- load_layers(outfolder, success)
          
          surface_os_vals <- terra::extract(layer, obis_sel_month[,coordnames], ID = F)
          
          na_to_solve <- which(is.na(surface_os_vals[,1]))
          
          surface_os_vals[na_to_solve, 1] <- get_nearby(obis_sel_month[na_to_solve, coordnames],
                                                             layer, mode = "24",
                                                             type = "mean")
          
          all_vals$flag[na_to_solve[!is.na(surface_os_vals[na_to_solve, 1])]] <- 
            all_vals$flag[na_to_solve[!is.na(surface_os_vals[na_to_solve, 1])]] + 512
          
          all_vals$ostiaSST <- surface_os_vals[,1]
          
        }

        fs::file_delete(list.files(outfolder, full.names = T))
        
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_ostia"] <- "concluded"
      } else {
        log_df[log_df$year == sel_year & log_df$month == sel_month, "status_ostia"] <- "unavailable"
      }

      
      all_vals <- left_join(obis_sel_month, all_vals, by = "temp_ID") %>%
        select(-temp_ID, -workID, -flagDate)
      
      have_data <- apply(all_vals %>%
                           select(surfaceTemperature, medianTemperature, bottomTemperature,
                                  minimumDepthTemperature, maximumDepthTemperature,
                                  coraltempSST, murSST),
                         1, function(x) any(!is.na(x)))
      
      all_vals <- all_vals[have_data,]
      
      if (nrow(all_vals) > 0) {
        write_parquet(all_vals, paste0(outfolder_final, "/", product, "_", sel_year, "_", sel_month, ".parquet"))
      }
      
      d_files <- list.files(outfolder, full.names = T)
      d_files <- d_files[grepl(paste0(sel_year, "_", sel_month), d_files)]
      fs::file_delete(d_files)
      
      log_df[log_df$year == sel_year & log_df$month == sel_month, "status_general"] <- "concluded"
      
    } else {
      log_df[log_df$year == sel_year & log_df$month == sel_month, "status_general"] <- "skipped"
    }
  }
}

fs::dir_create("logs")
write.csv(log_df, paste0("logs/log_", format(Sys.Date(), "%Y%m%d"), ".csv"), row.names = F)

# END