get_obis <- function() {
    f <- list.files("data", pattern = "obis_")
    if (length(f) < 1) {
        cat("Downloading full export...")
        api_call <- httr::content(httr::GET("https://api.obis.org/export?complete=true"), as = "parsed")
        api_call <- api_call$results[[1]]
        latest_export <- paste0("https://obis-datasets.ams3.digitaloceanspaces.com/", api_call$s3path)

        options(timeout = 999999999)
        f <- paste0("data/", gsub("exports/", "", api_call$s3path))
        download.file(
            url = latest_export,
            destfile = f,
            method = "wget"
        )
    }
    if (!file.exists(f)) {
        stop("File is not available. Check.")
    }
    return(f)
}

check_depth_diff <- function(depth_original, depth_new, limit = 5) {
    diff_depth <- abs(depth_original - depth_new)
    diff_depth <- ifelse(diff_depth > limit, TRUE, FALSE)
    ifelse(is.na(diff_depth), FALSE, diff_depth)
}

start_dask <- function(browse = TRUE) {
    da <- import("dask")
    dd <- import("dask.distributed")
    client <- dd$Client()
    if (browse) {
        browseURL("http://localhost:8787/status")
    } else {
        cat("For browsing, access http://localhost:8787/status\n")
    }
    return(client)
}
