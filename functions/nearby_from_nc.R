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

    lons <- coordinates$decimalLongitude
    lats <- coordinates$decimalLatitude

    if (length(lons) < 2) {
        lons <- list(lons)
        lats <- list(lats)
    }

    lats <- xr$DataArray(lats, dims = "z")
    lons <- xr$DataArray(lons, dims = "z")

    if (any(grepl("latitude", nams_coords))) {
        temp_res <- ds$sel(latitude = lats, longitude = lons, method = "nearest")
    } else {
        temp_res <- ds$sel(lat = lats, lon = lons, method = "nearest")
    }

    results <- temp_res$to_dataframe()
    nams_coords <- nams_coords[c(grep("lon", nams_coords), grep("lat", nams_coords))]
    results <- results[, c(nams_coords, variable)]
    colnames(results) <- c("actual_lon", "actual_lat", "value")

    results_final <- cbind(coordinates, results)
    results_final$value[is.nan(results_final$value)] <- NA

    return(results_final)
}


get_nearby <- function(netcdf, variable, coordinates, mode = "queen",
                       depth = NULL, date = NULL, verbose = TRUE) {

    if (verbose) cat("Getting nearby valid cells for", nrow(coordinates), "records\n")

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

    lats <- xr$DataArray(coordinates$decimalLatitude, dims = "z")
    lons <- xr$DataArray(coordinates$decimalLongitude, dims = "z")

    if (any(grepl("latitude", nams_coords))) {
        temp_res <- ds$sel(latitude = lats, longitude = lons, method = "nearest")
    } else {
        temp_res <- ds$sel(lat = lats, lon = lons, method = "nearest")
    }

    temp_res <- temp_res$to_dataframe()

    if (verbose && nrow(coordinates) > 1) {
        pb <- progress::progress_bar$new(total = nrow(coordinates))
        pbs <- TRUE
    } else {
        pbs <- FALSE
    }

    for (id in 1:nrow(coordinates)) {

        if (pbs) pb$tick()

        id_dim <- temp_res[id,]

        adj_x <- id_dim[,grep("lon", colnames(id_dim))]
        adj_y <- id_dim[,grep("lat", colnames(id_dim))]

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

        x_ids <- xr$DataArray(as.integer(vals_grid$x - 1), dims = "z")
        y_ids <- xr$DataArray(as.integer(vals_grid$y - 1), dims = "z")

        if ("longitude" %in% nams_coords) {
            vg_pt <- ds$isel(
                longitude = x_ids,
                latitude = y_ids
            )
            vg_pt <- vg_pt$to_dataframe()
        } else {
            vg_pt <- ds$isel(
                lon = x_ids,
                lat = y_ids
            )
            vg_pt <- vg_pt$to_dataframe()
        }

        if (nrow(vg_pt) > 0) {
            vals_grid$cx <- vg_pt[,grep("lon", colnames(id_dim))]
            vals_grid$cy <- vg_pt[,grep("lat", colnames(id_dim))]
            vals_grid$value <- vg_pt[[variable]]
            vals_grid$value[is.nan(vals_grid$value)] <- NA
        } else {
            vals_grid$value <- NA
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
