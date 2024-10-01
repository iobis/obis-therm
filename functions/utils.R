get_obis <- function() {
    f <- list.files("data", pattern = "parquet")
    if (length(f) < 1) {
        # Use export from other project if this one is not available
        f <- "../../mpa_europe/mpaeu_shared/obis_20231025.parquet"
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