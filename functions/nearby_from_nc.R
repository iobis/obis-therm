reticulate::source_python("functions/sort_dimension.py")

extract_from_nc <- function(netcdf, variable, coordinates, depth = NULL) {
    if (!exists("xr")) {
        stop("Xarray is not loaded. Load it using `xr <- reticulate::import('xarray')`")
    }

    ds <- xr$open_dataset(netcdf)
    ds <- ds[variable]
    ds <- ds$isel(time = 0L)
    if (!is.null(depth)) {
        ds <- ds$isel(depth = depth)
    }

    nams_coords <- unlist(ds$coords$dims)

    ds <- sort_dimension(ds, nams_coords[grepl("lat", nams_coords)])
    ds <- sort_dimension(ds, nams_coords[grepl("lon", nams_coords)])

    results <- lapply(1:nrow(coordinates), function(x) NULL)

    for (i in seq_len(nrow(coordinates))) {
        if (any(grepl("latitude", nams_coords))) {
            temp_res <- ds$sel(
                longitude = coordinates$decimalLongitude[i],
                latitude = coordinates$decimalLatitude[i],
                method = "nearest"
            )

            final_res <- data.frame(
                actual_lon = temp_res$longitude$item(),
                actual_lat = temp_res$latitude$item(),
                value = round(temp_res$item(), 1)
            )
        } else {
            temp_res <- ds$sel(
                lon = coordinates$decimalLongitude[i],
                lat = coordinates$decimalLatitude[i],
                method = "nearest"
            )

            final_res <- data.frame(
                actual_lon = temp_res$lon$item(),
                actual_lat = temp_res$lat$item(),
                value = round(temp_res$item(), 1)
            )
        }

        results[[i]] <- final_res
    }

    results <- do.call("rbind", results)

    results_final <- cbind(coordinates, results)
    results_final$value[is.nan(results_final$value)] <- NA

    return(results_final)
}


get_nearby <- function(netcdf, variable, coordinates, mode = "queen",
                       depth = NULL, date = NULL) {

    if (mode == "queen") {
        fadj = 1
    } else {
        fadj = 2
    }

    ds <- xr$open_dataset(netcdf)
    ds <- ds[variable]
    if (is.null(date)) {
        ds <- ds$isel(time = 0L)
    } else {
        ds <- ds$sel(time = date, method = "nearest")
        print(ds)
    }
    if (!is.null(depth)) {
        ds <- ds$isel(depth = depth)
    }

    nams_coords <- unlist(ds$coords$dims)

    ds <- sort_dimension(ds, nams_coords[grepl("lat", nams_coords)])
    ds <- sort_dimension(ds, nams_coords[grepl("lon", nams_coords)])

    if ("longitude" %in% nams_coords) {
        layer_x <- ds$longitude$to_dataframe()[,"longitude"]
        layer_y <- ds$latitude$to_dataframe()[,"latitude"]
    } else {
        layer_x <- ds$lon$to_dataframe()[,"lon"]
        layer_y <- ds$lat$to_dataframe()[,"lat"]
    }
    lim_x <- range(seq_along(layer_x))
    lim_y <- range(seq_along(layer_y))

    coordinates_idx <- coordinates
    coordinates_idx$value <- NA
    coordinates_idx$new_lon <- NA
    coordinates_idx$new_lat <- NA

    for (id in 1:nrow(coordinates)) {

        if ("longitude" %in% nams_coords) {
            id_dim <- ds$sel(
                longitude = coordinates$decimalLongitude[id],
                latitude = coordinates$decimalLatitude[id], method = "nearest"
            )
            adj_x <- id_dim$longitude$item()
            adj_y <- id_dim$latitude$item()
        } else {
            id_dim <- ds$sel(
                lon = coordinates$decimalLongitude[id],
                lat = coordinates$decimalLatitude[id], method = "nearest"
            )
            adj_x <- id_dim$lon$item()
            adj_y <- id_dim$lat$item()
        }

        adj_x <- which(adj_x == layer_x)
        adj_y <- which(adj_y == layer_y)

        adj_x <- seq(adj_x - fadj, adj_x + fadj)
        adj_y <- seq(adj_y - fadj, adj_y + fadj)

        adj_x[adj_x < lim_x[1]] <- lim_x[2] + adj_x[adj_x < lim_x[1]]
        adj_x[adj_x > lim_x[2]] <- adj_x[adj_x > lim_x[2]] - lim_x[2]

        adj_x <- adj_x[adj_x >= lim_x[1] & adj_x <= lim_x[2]]
        adj_y <- adj_y[adj_y >= lim_y[1] & adj_y <= lim_y[2]]

        vals_grid <- expand.grid(x = adj_x, y = adj_y)
        vals_grid$value <- vals_grid$cy <- vals_grid$cx <- NA

        for (vg in 1:nrow(vals_grid)) {
            if ("longitude" %in% nams_coords) {
                vg_pt <- ds$isel(
                    longitude = as.integer(vals_grid$x[vg] - 1),
                    latitude = as.integer(vals_grid$y[vg] - 1)
                )
                vg_pt <- data.frame(x = vg_pt$longitude$item(), y = vg_pt$latitude$item(),
                                    value = ifelse(is.nan(vg_pt$item()), NA, vg_pt$item()))
            } else {
                vg_pt <- ds$isel(
                    lon = as.integer(vals_grid$x[vg] - 1),
                    lat = as.integer(vals_grid$y[vg] - 1)
                )
                vg_pt <- data.frame(x = vg_pt$lon$item(), y = vg_pt$lat$item(),
                                    value = ifelse(is.nan(vg_pt$item()), NA, vg_pt$item()))
            }
            #vg_pt <- as.data.frame(layer[1, vals_grid$x[vg], vals_grid$y[vg]])
            if (nrow(vg_pt) > 0) {
                vals_grid$cx[vg] <- vg_pt$x[1]
                vals_grid$cy[vg] <- vg_pt$y[1]
                vals_grid$value[vg] <- vg_pt$value[1]
            } else {
                vals_grid$value[vg] <- NA
            }
        }

        if (all(is.na(vals_grid$value))) {
            coordinates_idx$new_lon[id] <- coordinates_idx$decimalLongitude[id]
            coordinates_idx$new_lat[id] <- coordinates_idx$decimalLatitude[id]
            coordinates_idx$value[id] <- NA
        } else {
            if (sum(!is.na(vals_grid$value)) == 1) {
                sel_val <- vals_grid[!is.na(vals_grid$value),]
                coordinates_idx$new_lon[id] <- sel_val$cx[1]
                coordinates_idx$new_lat[id] <- sel_val$cy[1]
                coordinates_idx$value[id] <- sel_val$value[1]
            } else {
                sel_val <- vals_grid[!is.na(vals_grid$value),]
                true_x <- coordinates_idx$decimalLongitude[id]
                true_y <- coordinates_idx$decimalLatitude[id]
                sel_val$diff <- abs(sel_val$cy - true_y) + abs(sel_val$cx - true_x)
                sel_val$diff[sel_val$diff >= 360] <- sel_val$diff[sel_val$diff >= 360] - 360
                sel_val <- sel_val[order(sel_val$diff), ]
                sel_val <- sel_val[1,]

                coordinates_idx$new_lon[id] <- sel_val$cx[1]
                coordinates_idx$new_lat[id] <- sel_val$cy[1]
                coordinates_idx$value[id] <- sel_val$value[1]
            }
        }
    }

    return(coordinates_idx)
}
