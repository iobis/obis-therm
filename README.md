# OBIS - monthly temperature dataset

This repository contains the code used to generate the OBIS dataset, which includes occurrence data matched with multiple sources of monthly temperature. Temperature data is extracted for each occurrence based on the date it was collected, at the recorded depth or across multiple depths. See how to download it [here](https://github.com/iobis/obis-therm#accessing-the-dataset) and how to use it [here](https://github.com/iobis/obis-therm#using-the-data).

## Temperature sources

At this moment, the dataset include temperature information from four sources:

- [Global Ocean Physics Reanalysis (CMEMS - GLORYS)](https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description) - The GLORYS product is the CMEMS global ocean eddy-resolving reanalysis (1993 onward). It is based on the current real-time global forecasting CMEMS system. The model component is the NEMO platform. This is a modeled product, with 50 vertical levels and offered at a 1/12° resolution (equirectangular grid). This is a L4 product.
- [Daily Global 5km Satellite Sea Surface Temperature (NOAA - CoralTemp)](https://coralreefwatch.noaa.gov/product/5km/index_5km_sst.php) - The NOAA Coral Reef Watch (CRW) daily global 5km Sea Surface Temperature (SST) product (CoralTemp), shows the nighttime ocean temperature measured at the surface. The product was developed from two related reanalysis (i.e. reprocessed) SST products and a near real-time SST product.
- [Multi-Scale Ultra High Resolution Sea Surface Temperature (NASA - MUR-SST)](https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1) - MUR provides global SST data every day at a spatial resolution of 0.01 degrees in longitude-latitude coordinates, roughly at 1 km intervals. The MUR dataset is among the highest resolution SST analysis datasets currently available.
- [Global Ocean OSTIA Sea Surface Temperature and Sea Ice Analysis](https://data.marine.copernicus.eu/product/SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/description) - The OSTIA global foundation Sea Surface Temperature product provides daily gap-free maps of Foundation Sea Surface Temperature at 0.05° grid resolution, using in-situ and satellite data from both infrared and microwave radiometers. The OSTIA system is run by the UK's Met Office and delivered by IFREMER PU. This is a L4 product.

## Codes

The production of this dataset is simple and depends on a single code: `get_temperatures.R` (and associated functions). Ensure that all requirements are met (see `requirements.R`).

Once the data is downloaded, the separate `parquet` files are aggregated and the H3 index is added. This is done through the `aggregate_files.R`

For downloading data from Copernicus you will need a valid account (you can create one for free [here](https://data.marine.copernicus.eu/register)). You should then store your credentials on the environment using the following:

```
usethis::edit_r_environ()
```

And then add:

```
COPERNICUS_USER="your user"
COPERNICUS_PWD="your password"
```

Alternatively, you can supply the credentials directly in the code.

## Accessing the dataset

The final dataset is available through the OBIS AWS S3 bucket `s3://obis-products/obis-therm`. If you have the **AWS** CLI program installed in your computer, you can run the following in the command line:

``` batch
aws s3 cp --recursive s3://obis-products/obis-therm . --no-sign-request
```
What will download all files to your local folder. Alternatively, on R you can use the `aws.s3` package:

``` r
library(aws.s3)

local_folder <- "obis-therm"
fs::dir_create(local_folder)

bucket <- "obis-products"
s3_folder <- "obis-therm"
s3_objects <- get_bucket(bucket = bucket, prefix = s3_folder, use_https = TRUE, max = Inf)

i <- 0
total <- length(s3_objects)
for (obj in s3_objects) {
    i <- i + 1
    cat("Downloading", i, "out of", total, "\n")
    s3_key <- obj$Key
    local_file <- file.path(local_folder, gsub("results/species/", "", s3_key))

    if (!endsWith(s3_key, "/")) {
        save_object(
            object = s3_key,
            bucket = bucket,
            file = local_file,
            region = "",
            use_https = TRUE 
        )
        message(paste("Downloaded:", s3_key, "to", local_file))
    }
}
```

## Using the data

``` r
library(arrow)
library(dplyr)

ds <- open_dataset("obis-therm") # path to the dataset

acanthuridae <- ds %>%
    filter(family == "Acanthuridae") %>%
    collect()

head(acanthuridae)
```

More details soon.


## Updates

- The code now uses `Dask` integration with `xarray` for parallel processing.

## Next steps

- Convert all main code to Python, and just do post-processing on R: that would be ideal, so we can use for example Dask for parallel processing.