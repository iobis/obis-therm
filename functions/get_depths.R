reticulate::source_python("functions/sort_dimension.py")

get_depths <- function(dataset, target_data, sel_date, variable = "thetao", verbose = TRUE) {

    date_ds <- dataset[variable]$sel(
        time = sel_date,
        method = "nearest"
    )

    nams_coords <- unlist(date_ds$coords$dims)

    date_ds <- sort_dimension(date_ds, nams_coords[grepl("lat", nams_coords)])
    date_ds <- sort_dimension(date_ds, nams_coords[grepl("lon", nams_coords)])

    lons <- xr$DataArray(target_data$decimalLongitude, dims = "z")
    lats <- xr$DataArray(target_data$decimalLatitude, dims = "z")

    temp_data <- date_ds$sel(
        longitude = lons,
        latitude = lats,
        method = "nearest"
    )

    lon_indices <- date_ds$get_index('longitude')$get_indexer(temp_data['longitude'])
    lat_indices <- date_ds$get_index('latitude')$get_indexer(temp_data['latitude'])

    rm(temp_data)

    indices <- data.frame(x = lon_indices, y = lat_indices, id = seq_len(length(lon_indices)))
    indices <- indices %>%
        group_by(x, y) %>%
        mutate(unique_id = cur_group_id())

    un_indices <- indices %>%
        select(-id) %>%
        distinct(unique_id, .keep_all = T)

    dataset_depths <- data.frame(unique_id = un_indices$unique_id, depth_deep = NA, depth_mid = NA)

    if (verbose && nrow(dataset_depths) > 1) {
        cat("Extracting depths...\n")
        pb <- progress::progress_bar$new(total = nrow(dataset_depths))
        pbs <- TRUE
    } else {
        pbs <- FALSE
    }

    av_depths <- date_ds['depth']$values

    for (i in 1:nrow(dataset_depths)) {
        if (pbs) pb$tick()

        ext_depth <- date_ds$isel(
            longitude = as.integer(un_indices$x[i]),
            latitude = as.integer(un_indices$y[i])
        )

        if (ext_depth$notnull()$any()$item()) {
            non_na <- av_depths[ext_depth$notnull()$values]
        } else {
            non_na <- c()
        }

        if (length(non_na) > 0) {
            max_depth <- max(non_na)
            surface_depth <- min(non_na)
            mid_value <- (max_depth + surface_depth) / 2
            mid_depth <- non_na[which.min(abs(non_na - mid_value))]
        } else {
            max_depth <- mid_depth <- NA
        }

        dataset_depths$depth_deep[i] <- max_depth
        dataset_depths$depth_mid[i] <- mid_depth
    }

    dataset_depths <- dplyr::left_join(indices, dataset_depths, by = "unique_id")

    dataset_depths <- dataset_depths[order(dataset_depths$id),]

    dataset_depths <- dataset_depths %>% ungroup() %>% select(id, depth_deep, depth_mid)

    return(dataset_depths)
}
