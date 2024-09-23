get_depths <- function(dataset, target_data, sel_date) {
    date_ds <- dataset$thetao$sel(
        time = sel_date,
        method = "nearest"
    )

    dataset_depths <- lapply(1:nrow(target_data), function(x) NULL)

    for (i in 1:nrow(target_data)) {
        ext_depth <- date_ds$sel(
            longitude = as.numeric(target_data[i, "decimalLongitude"]),
            latitude = as.numeric(target_data[i, "decimalLatitude"]),
            method = "nearest"
        )
        ext_depth <- ext_depth$to_dataframe()

        if (i == 1) {
            depths <- as.numeric(row.names(ext_depth))
        }

        non_na <- depths[!is.na(ext_depth$thetao)]

        if (length(non_na) > 0) {
            max_depth <- max(non_na)
            surface_depth <- min(non_na)
            mid_value <- (max_depth + surface_depth) / 2
            mid_dep <- non_na[which.min(abs(non_na - mid_value))]
        } else {
            max_depth <- mid_dep <- NA
        }

        dataset_depths[[i]] <- data.frame(
            temp_ID = target_data$temp_ID[i],
            max_depth = max_depth, mid_dep = mid_dep
        )
    }

    return(do.call("rbind", dataset_depths))
}