######################### OBIS sea temperature dataset ########################
# January of 2024
# Author: Silas C. Principe
# Contact: s.principe@unesco.org
#
################################ Data aggregation ##############################

library(arrow)
library(dplyr)

aggregate_data <- function(input_folder, output_folder,
                           h3_resolutions = c(7)) {
  
  file_schema <- schema(read_parquet(list.files(input_folder, full.names = T)[1]))
  file_schema$surfaceTemperature <- double()
  file_schema$medianTemperature <- double()
  file_schema$bottomTemperature <- double()
  file_schema$medianDepth <- double()
  file_schema$bottomDepth <- double()
  file_schema$minimumDepthTemperature <- double()
  file_schema$maximumDepthTemperature <- double()
  file_schema$minimumDepthClosestDepth <- double()
  file_schema$maximumDepthClosestDepth <- double()
  file_schema$coraltempSST <- double()
  file_schema$murSST <- double()
  file_schema$ostiaSST <- double()
  
  ds <- open_dataset(input_folder,
                     schema = file_schema)
  
  ds %>%
    select(-date_start, -date_end, -date_year,
           -extractedDateStart, -extractedDateMid, -extractedDateEnd) %>%
    rename(year = extractedDateYear, month = extractedDateMonth) %>%
    mutate(flag = as.integer(flag), 
           year = as.integer(year), 
           month = as.integer(month),
           surfaceTemperature = round(surfaceTemperature, 2),
           medianTemperature = round(medianTemperature, 2),
           bottomTemperature = round(bottomTemperature, 2),
           minimumDepthTemperature = round(minimumDepthTemperature, 2),
           maximumDepthTemperature = round(maximumDepthTemperature, 2),
           coraltempSST = round(coraltempSST, 2),
           murSST = round(murSST, 2),
           ostiaSST = round(ostiaSST, 2)) %>%
    group_by(year) %>%
    write_dataset(path = output_folder)

  tf <- list.files(output_folder, recursive = T, full.names = T)

  pb <- progress::progress_bar$new(total = length(tf))

  for (id in seq_along(tf)) {
    pb$tick()

    tf_content <- read_parquet(tf[id])
           
    tf_sf <- sf::st_as_sf(tf_content, 
                          coords = c("decimalLongitude", "decimalLatitude"),
                          crs = 4326,
                          remove = FALSE
                          )
    
    for (hr in h3_resolutions) {
      batches <- split(seq_len(nrow(tf_sf)), ceiling(seq_len(nrow(tf_sf)) / 10000))

      cell_values <- lapply(batches, function(bt) {
        h3jsr::point_to_cell(tf_sf[bt,], res = hr)
      })
      cell_values <- unlist(cell_values, use.names = F)

      tf_sf[[paste0("h3_", hr)]] <- cell_values
    }
    
    suppressWarnings(sfarrow::st_write_parquet(tf_sf, tf[id]))

  }
  
  cat("Aggregation concluded \n")
    
  return(invisible(NULL))
  
}

# Settings
input_folder <- "results/"
output_folder <- "aggregated/"
fs::dir_create(output_folder)

# Aggregate
aggregate_data(input_folder, output_folder)

### END