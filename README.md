# OBIS - monthly temperature dataset

This repository contains the code used to generate the OBIS dataset, which includes occurrence data matched with multiple sources of monthly temperature. Temperature data is extracted for each occurrence based on the date it was collected, at the recorded depth or across multiple depths.

## Temperature sources

At this moment, the dataset include temperature information from three sources:

- [Global Ocean Physics Reanalysis (CMEMS - GLORYS)](https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description)  
- [Daily Global 5km Satellite Sea Surface Temperature (NOAA - CoralTemp)](https://coralreefwatch.noaa.gov/product/5km/index_5km_sst.php)  
- [Multi-Scale Ultra High Resolution Sea Surface Temperature (NASA - MUR-SST)](https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1)

## Codes

The production of this dataset is simple and depends on a single code: `get_temperatures.R` (and associated functions). Ensure that all requirements are met (see `requirements.R`).

Once the data is downloaded, the separate `parquet` files are aggregated and the H3 index is added. This is done through the `aggregate_files.R`

For downloading data from Copernicus you will need a valid account (you can create one for free [here](https://data.marine.copernicus.eu/register)). You should then store your credentials on the environment using the following:

```
usethis::edit_r_environ()
```

And then add:

```
COPERNICUS_USER="your user")
COPERNICUS_PWD="your password")
```

Alternatively, you can supply the credentials directly in the code.

## Accessing the dataset

The final dataset will be available through the OBIS AWS S3 bucket (to be added soon).