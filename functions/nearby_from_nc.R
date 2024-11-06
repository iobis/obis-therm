reticulate::source_python("functions/sort_dimension.py")

extract_from_nc <- function(netcdf, variable, coordinates, depth = NULL) {
    if (!exists("xr")) {
        stop("Xarray is not loaded. Load it using `xr <- reticulate::import('xarray')`")
    }

    ds <- xr$open_dataset(netcdf, chunks = "auto")
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

    ds <- xr$open_dataset(netcdf, chunks = "auto")
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

    lons <- coordinates$decimalLongitude
    lats <- coordinates$decimalLatitude

    if (length(lons) < 2) {
        lons <- list(lons)
        lats <- list(lats)
    }

    lons <- xr$DataArray(lons, dims = "z")
    lats <- xr$DataArray(lats, dims = "z")

    if (any(grepl("latitude", nams_coords))) {
        temp_res <- ds$sel(latitude = lats, longitude = lons, method = "nearest")
    } else {
        temp_res <- ds$sel(lat = lats, lon = lons, method = "nearest")
    }

    temp_res <- temp_res$to_dataframe()

    extra_coords <- lapply(seq_len(nrow(temp_res)), function(x) NULL)

    if (verbose && nrow(coordinates) > 1) {
        pb <- progress::progress_bar$new(total = nrow(temp_res))
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
        vals_grid$ID <- id
        extra_coords[[id]] <- vals_grid
    }

    if (verbose) cat("Processing adjacent points...\n")
    extra_coords <- do.call("rbind", extra_coords)

    x_ids <- xr$DataArray(as.integer(extra_coords$x - 1), dims = "z")
    y_ids <- xr$DataArray(as.integer(extra_coords$y - 1), dims = "z")

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

    if (nrow(extra_coords) != nrow(vg_pt)) stop("Error on data extraction.")

    extra_coords$cx <- vg_pt[,grep("lon", colnames(vg_pt))]
    extra_coords$cy <- vg_pt[,grep("lat", colnames(vg_pt))]
    extra_coords$value <- vg_pt[[variable]]
    extra_coords$value[is.nan(extra_coords$value)] <- NA

    coordinates_idx$ID <- seq_len(nrow(coordinates_idx))
    extra_coords <- dplyr::left_join(extra_coords,
                                     coordinates_idx[,c("decimalLongitude", "decimalLatitude", "ID")], by = "ID")

    tfun <- function(cx, cy, value, decimalLongitude, decimalLatitude) {
        if (all(is.na(value))) {
            df <- data.frame(new_lon = NA, new_lat = NA, value = NA)
        } else {
            if (sum(!is.na(value)) == 1) {
                df <- data.frame(new_lon = cx[!is.na(value)],
                                 new_lat = cy[!is.na(value)],
                                 value = value[!is.na(value)])
            } else {
                cx <- cx[!is.na(value)]
                cy <- cy[!is.na(value)]
                value <- value[!is.na(value)]

                true_x <- decimalLongitude[1]
                true_y <- decimalLatitude[1]
                diff <- abs(cy - true_y) + abs(cx - true_x)
                diff[diff >= 360] <- diff[diff >= 360] - 360

                df <- data.frame(new_lon = cx[order(diff)][1],
                                 new_lat = cy[order(diff)][1],
                                 value = value[order(diff)][1])
            }
        }
        return(dplyr::as_tibble(df))
    }
    
    extra_coords <- extra_coords %>%
        group_by(ID) %>%
        summarise(tfun(cx, cy, value, decimalLongitude, decimalLatitude))

    coordinates_idx <- bind_cols(
        coordinates_idx[,c("decimalLongitude", "decimalLatitude")],
        extra_coords[,c("value", "new_lon", "new_lat", "ID")]
    )

    return(coordinates_idx)
}
