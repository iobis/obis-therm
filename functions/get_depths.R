reticulate::source_python("functions/sort_dimension.py")

get_depths <- function(dataset, target_data, sel_date, variable = "thetao", verbose = TRUE) {

    if (verbose) cat("Getting depths for", nrow(target_data), "points.\n")

    it <- Sys.time()

    if (nrow(target_data) < 2) { # Case for 1 record, which was not working well with Xarray

        date_ds <- dataset[variable]$sel(
            time = sel_date,
            method = "nearest"
        )

        temp_data <- date_ds$sel(
            longitude = target_data$decimalLongitude,
            latitude = target_data$decimalLatitude,
            method = "nearest"
        )

        temp_df <- temp_data$to_dataframe()
        depths <- temp_data$depth$values
        values <- temp_df$thetao

        valid <- c(min(which(!is.na(values))), max(which(!is.na(values))))
        depths_v <- depths[valid]

        max_d <- max(depths_v)
        min_d <- min(depths_v)

        if (is.na(max_d) || length(max_d) < 1) {
            depth_deep <- depth_mid <- NA
        } else {
            mid_value <- (max_d + min_d) / 2
            depth_mid <- depths[which.min(abs(depths - mid_value))]
            if (length(depth_mid) < 1 || is.na(depth_mid)) {
                depth_mid <- NA
            }
            depth_deep <- max_d
        }

        dataset_depths <- data.frame(
            temp_ID = target_data$temp_ID,
            depth_deep = depth_deep,
            depth_mid = depth_mid
        )

    } else {
        np <- import("numpy")

        date_ds <- dataset[variable]$sel(
            time = sel_date,
            method = "nearest"
        )

        nams_coords <- unlist(date_ds$coords$dims)

        date_ds <- sort_dimension(date_ds, nams_coords[grepl("lat", nams_coords)])
        date_ds <- sort_dimension(date_ds, nams_coords[grepl("lon", nams_coords)])

        lons <- target_data$decimalLongitude
        lats <- target_data$decimalLatitude

        if (length(lons) < 2) {
            lons <- list(lons)
            lats <- list(lats)
        }

        lons <- xr$DataArray(lons, dims = "z")
        lats <- xr$DataArray(lats, dims = "z")

        temp_data <- date_ds$sel(
            longitude = lons,
            latitude = lats,
            method = "nearest"
        )

        av_depths <- date_ds['depth']$values

        # Max refers to the top, so it is actually the min
        min_depths <- temp_data$notnull()$argmax(dim = "depth")$where(temp_data$notnull()$any(dim="depth"), np$nan)
        max_depths <- temp_data$notnull()$argmin(dim = "depth")$where(temp_data$notnull()$any(dim="depth"), np$nan)

        max_depths <- (max_depths - 1)$where(max_depths > 0, np$nan)

        min_depth_values <- temp_data['depth']$isel(depth = 
            min_depths$fillna(-1)$astype(np$int_))$where(min_depths$notnull(), np$nan)
        max_depth_values <- temp_data['depth']$isel(depth = 
            max_depths$fillna(-1)$astype(np$int_))$where(max_depths$notnull(), np$nan)

        if (verbose) cat("Extracting values...\n")

        min_depths <- min_depth_values$values
        max_depths <- max_depth_values$values

        min_depths[is.nan(min_depths)] <- NA
        max_depths[is.nan(max_depths)] <- NA

        mid_value <- (max_depths + min_depths) / 2
        mid_depths <- unlist(lapply(
            mid_value, function(x) {
                va <- av_depths[which.min(abs(av_depths - x))]
                if (length(va) < 1 || is.na(va)) {
                    va <- NA
                }
                return(va)
            }
        ))

        dataset_depths <- data.frame(
            temp_ID = target_data$temp_ID,
            depth_deep = max_depths,
            depth_mid = mid_depths#,
            # longitude = temp_data$longitude$values,
            # latitude = temp_data$latitude$values
        )
    }

    if (verbose) cat(difftime(Sys.time(), it, units = "secs"), "sec elapsed.\n")

    return(dataset_depths)
}


### Validation with terra
# dataset_depths <- data.frame(
#         temp_ID = target_data$temp_ID,
#         depth_deep = max_depths,
#         depth_mid = mid_depths,
#         longitude = temp_data$longitude$values,
#         latitude = temp_data$latitude$values
#     )

# r <- rast("temp/mercatorglorys12v1_gl12_mean_200001.nc")
# r <- subset(r, names(r)[grepl("thetao", names(r))])
# r
# names(r)

# r_depths <- as.numeric(gsub("thetao_depth=", "", names(r)))

# ex <- terra::extract(r, as.data.frame(dataset_depths[,c("longitude", "latitude")]), ID = F)
# ex_m <- apply(ex, 1, function(x){
#     if (all(is.na(x))) {
#         NA
#     } else {
#         ix <- max(which(!is.na(x)))
#         if (r_depths[ix] == min(r_depths)) {
#             NA
#         } else {
#             r_depths[ix]
#         }
#     }
# })
# ex_mid <- unlist(lapply(ex_m, function(x){
#     va <- r_depths[which.min(abs(r_depths - ((x+min(r_depths))/2)))]
#     if (length(va) < 1 || is.na(va)) {
#         va <- NA
#     }
#     return(va)
# }))

# dataset_depths$rast_version <- ex_m
# dataset_depths$rast_version_mid <- ex_mid

# head(dataset_depths)
# summary(dataset_depths)

# all.equal(dataset_depths$depth_deep, dataset_depths$rast_version)
# all.equal(dataset_depths$depth_mid, dataset_depths$rast_version_mid)
# dataset_depths[dataset_depths$depth_mid != dataset_depths$rast_version_mid,c("depth_mid", "rast_version_mid")]
# all.equal(round(dataset_depths$depth_mid, 4), round(dataset_depths$rast_version_mid, 4))
