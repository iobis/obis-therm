subset_grid <- function(dataset, grid_res = c(8, 8), 
                        crd_name = c("decimalLongitude", "decimalLatitude")) {
  
  grid <- terra::rast(nrow = grid_res[2], ncol = grid_res[1])
  
  grid[] <- 1:ncell(grid)
  
  id <- terra::extract(grid, dataset[,crd_name], ID = F)
  
  dataset$workID <- id[,1]
  
  return(dataset)  
}

