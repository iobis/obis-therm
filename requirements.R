##################### OBIS - species thermal information #######################
# August of 2024
# Authors: Silas C. Principe, Pieter Provoost
# Contact: s.principe@unesco.org
############################ Project Requirements ##############################

# Needed packages on CRAN
req_packages <- c(
  "terra",
  "sf",
  "sfarrow",
  "h3jsr",
  "reticulate",
  "arrow",
  "parallel",
  "dplyr",
  "tidyr",
  "rerddap",
  "ncdf4",
  "fs",
  "httr",
  "storr",
  "progress",
  "httr"
)

# Create a function to check if is installed
is_package_installed <- function(pkg) {
  requireNamespace(pkg, quietly = TRUE)
}

# Check which ones are not installed and install if needed:
for (i in 1:length(req_packages)) {
  if (!is_package_installed(req_packages[i])) {
    install.packages(req_packages[i])
  }
}

# Install Python packages
reticulate::py_install(c("xarray", "zarr", "copernicusmarine"), pip = TRUE)

# Download full export of OBIS
api_call <- httr::content(httr::GET("https://api.obis.org/export?complete=true"), as = "parsed")
api_call <- api_call$results[[1]]
latest_export <- paste0("https://obis-datasets.ams3.digitaloceanspaces.com/", api_call$s3path)

options(timeout = 999999999)
fs::dir_create("data")
download.file(
    url = latest_export,
    destfile = paste0("data/", gsub("exports/", "", api_call$s3path)),
    method = "wget"
)