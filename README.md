# OBIS - monthly temperature dataset

This repository contains the code used to generate the OBIS dataset, which includes occurrence data matched with multiple sources of monthly temperature. Temperature data is extracted for each occurrence based on the date it was collected, at the recorded depth or across multiple depths.

## Temperature sources

At this moment, the dataset include temperature information from three sources:

- [Global Ocean Physics Reanalysis (CMEMS - GLORYS)](https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description)  
- [Daily Global 5km Satellite Sea Surface Temperature (NOAA - CoralTemp)](https://coralreefwatch.noaa.gov/product/5km/index_5km_sst.php)  
- [Multi-Scale Ultra High Resolution Sea Surface Temperature (NASA - MUR-SST)](https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1)

## Codes

The production of this dataset is simple and depends on a single code: `get_temperatures.R` (and associated functions. Ensure that all requirements are met (see `requirements.R`).

## Accessing the dataset

The final dataset will be available through the OBIS AWS S3 bucket (to be added soon).