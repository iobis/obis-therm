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