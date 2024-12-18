# OBIS - monthly temperature dataset

This repository contains the code used to generate the **obistherm** dataset, which includes OBIS occurrence data matched with multiple sources of monthly temperature. Temperature data is extracted for each occurrence based on the date it was collected, at the recorded depth or across multiple depths. See how to download it [here](https://github.com/iobis/obis-therm#accessing-the-dataset) and how to use it [here](https://github.com/iobis/obis-therm#using-the-data).

You can understand the dataset structure [here](https://github.com/iobis/obis-therm/blob/main/structure.md). The current version of **obistherm** is based on the [OBIS full export](https://obis.org/data/access/) of 2024-07-23 and covers the period of 1986 to 2024.

## Temperature sources

At this moment, the dataset include temperature information from four sources:

- [Global Ocean Physics Reanalysis (CMEMS - GLORYS)](https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description) - The GLORYS product is the CMEMS global ocean eddy-resolving reanalysis (1993 onward). It is based on the current real-time global forecasting CMEMS system. The model component is the NEMO platform. This is a modeled product, with 50 vertical levels and offered at a 1/12° resolution (equirectangular grid). This is a L4 product.
- [Daily Global 5km Satellite Sea Surface Temperature (NOAA - CoralTemp)](https://coralreefwatch.noaa.gov/product/5km/index_5km_sst.php) - The NOAA Coral Reef Watch (CRW) daily global 5km Sea Surface Temperature (SST) product (CoralTemp), shows the nighttime ocean temperature measured at the surface (1986 onward). The product was developed from two related reanalysis (i.e. reprocessed) SST products and a near real-time SST product.
- [Multi-Scale Ultra High Resolution Sea Surface Temperature (NASA - MUR-SST)](https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1) - MUR provides global SST data every day at a spatial resolution of 0.01 degrees in longitude-latitude coordinates, roughly at 1 km intervals (2002 onward). The MUR dataset is among the highest resolution SST analysis datasets currently available.
- [Global Ocean OSTIA Sea Surface Temperature and Sea Ice Analysis](https://data.marine.copernicus.eu/product/SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/description) - The OSTIA global foundation Sea Surface Temperature product provides daily gap-free maps of Foundation Sea Surface Temperature at 0.05° grid resolution, using in-situ and satellite data from both infrared and microwave radiometers (2007 onward). The OSTIA system is run by the UK's Met Office and delivered by IFREMER PU. This is a L4 product.

## Codes

The production of this dataset is simple and depends on a single code: `get_temperatures.R` (and associated functions). An overview of the production steps is available [here](https://github.com/iobis/obis-therm/blob/main/pseudocode.md). Before starting, ensure that all requirements are met (see `requirements.R`).

Once the data is downloaded, the separate `parquet` files are aggregated, the H3 index is added, and the file is converted to GeoParquet. This is done through the `aggregate_files.R`

For downloading data from Copernicus you will need a valid account (you can create one for free [here](https://data.marine.copernicus.eu/register)). You should then store your credentials on the environment using the following:

``` r
usethis::edit_r_environ()
```

And then add:

``` r
COPERNICUS_USER="your user"
COPERNICUS_PWD="your password"
```

Alternatively, you can supply the credentials directly in the code.

## Accessing the dataset

The final dataset is available through the OBIS AWS S3 bucket `s3://obis-products/obistherm`. If you have the **AWS** CLI program installed in your computer, you can run the following in the command line:

``` bash
aws s3 cp --recursive s3://obis-products/obistherm . --no-sign-request
```
What will download all files to your local folder. Alternatively, on R you can use the `aws.s3` package:

``` r
library(aws.s3)

local_folder <- "obistherm"
fs::dir_create(local_folder)

bucket <- "obis-products"
s3_folder <- "obistherm"
s3_objects <- get_bucket(bucket = bucket, prefix = s3_folder, use_https = TRUE, max = Inf)

i <- 0
total <- length(s3_objects)
for (obj in s3_objects) {
    i <- i + 1
    cat("Downloading", i, "out of", total, "\n")
    s3_key <- obj$Key
    local_file <- file.path(local_folder, s3_key)

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

The data is stored as a [GeoParquet](https://geoparquet.org/) file. You can read it using [`arrow`](https://arrow.apache.org/), or with geospatial libraries (e.g. Python `GeoPandas`, R `sfarrow`). Although you can access it directly through the S3 bucket, it is always faster when you have a local copy stored.

Accessing through `arrow` (no spatial):

``` r
library(arrow)
library(dplyr)

ds <- open_dataset("obistherm") # path to the dataset

acanthuridae <- ds %>%
    filter(family == "Acanthuridae") %>%
    collect()

head(acanthuridae)
```

With `sfarrow` (spatial, returns an `sf` object):

``` r
library(arrow)
library(dplyr)
library(sfarrow)

ds <- open_dataset("obistherm") # path to the dataset

acanthuridae <- ds %>%
    filter(family == "Acanthuridae")

acanthuridae <- read_sf_dataset(acanthuridae)

acanthuridae
```

You can also use the function `retrieve_data` which is [provided in this repo.](https://github.com/iobis/obis-therm/blob/main/functions/retrieve_data.R) It will use DuckDB for fast querying.

``` r
source("https://raw.githubusercontent.com/iobis/obis-therm/refs/heads/main/functions/retrieve_data.R")

# If you don't pass any value to datasource, it will use the S3 access point
lthay <- retrieve_data(scientificname = "Leptuca thayeri", year = 2020)

# With a local source
lthay <- retrieve_data(scientificname = "Leptuca thayeri", year = 2020,
                       datasource = "obistherm")

# Use return_query = T to return only the DBI/DuckDB query, that you can use to access the data
lthay <- retrieve_data(scientificname = "Leptuca thayeri", year = 2020,
                       return_query = T)

# Example using duckDB with the S3 access point:
query <- "
select *
  from read_parquet('s3://obis-products/obistherm/*/*')
  where year = 2020 and species = 'Leptuca thayeri'
"

con <- dbConnect(duckdb())
dbSendQuery(con, "install httpfs; load httpfs;")
result <- dbGetQuery(con, query)
dbDisconnect(con)

head(result)
```

On Python, you can use GeoPandas:

``` python
import geopandas as gpd

species_filter = [("species", "==", "Acanthurus chirurgus")]
year_filter = [("year", "==", 2000)]

gdf = gpd.read_parquet("obistherm/", filters=species_filter + year_filter)[["geometry", "species", "coraltempSST", "year"]]

gdf
```


## Examples

NOTE: on all examples, we use a local copy stored in a folder called "aggregated". Change it to your local folder or access through the S3 access point.

### R

Temperature data for two fiddler crab species (family Ocypodidae):

``` r
library(ggplot2)

ocy <- retrieve_data(scientificname = c(
    "Leptuca thayeri", "Minuca rapax"
), datasource = "aggregated/") # Change here with your data source or NULL
# to use the S3 access point

ocy_proc <- ocy %>%
    mutate(date = as.Date(paste(year, month, "01", sep = "-"))) %>%
    rename(glorysSST = surfaceTemperature) %>%
    tidyr::pivot_longer(cols = ends_with("SST"),
                        names_to = "SSTsource", values_to = "sst")

ggplot(ocy_proc) +
    geom_point(aes(x = date, y = sst, color = SSTsource), alpha = .5) +
    geom_smooth(aes(x = date, y = sst, group = SSTsource),
                method = "lm", color = "grey30") +
    scale_color_manual(values = c("#11b5ae", "#4046ca", "#f68512", "#de3c82")) +
    xlab(NULL) + ylab("Temperature (°C)") +
    theme_light() +
    theme(
        panel.grid.major.x = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        legend.position = "none",
        strip.text.x = element_text(face = "italic")
    ) +
    facet_grid(SSTsource ~ species)
```
![](images/fiddlers.png)


Spatial plots can be done very easily, as the dataset is in GeoParquet format.

``` r
library(arrow)
library(dplyr)
library(ggplot2)
library(sf)

ds <- open_dataset("aggregated")

acanthurus <- ds %>%
    select(species, coraltempSST, geometry, h3_7, year, month) %>%
    filter(species == "Acanthurus coeruleus") %>%
    filter(!is.na(coraltempSST)) %>%
    filter(year == 2014)

acanthurus <- sfarrow::read_sf_dataset(acanthurus)

period <- data.frame(
    period = rep(c("T1", "T2", "T3", "T4"), each = 3),
    month = 1:12
)

acanthurus <- left_join(acanthurus, period)

ggplot() +
    geom_sf(data = rnaturalearth::ne_countries(returnclass = "sf"),
            color = "gray80", fill = "gray80") +
    geom_sf(data = acanthurus, aes(color = coraltempSST, shape = period), alpha = .5, size = 2) +
    scale_color_viridis_c() +
    coord_sf(xlim = c(-95, -30), ylim = c(-30, 30)) +
    ggtitle("Acanthurus choeruleus", "Year: 2014 - Product: CoralTemp (NOAA)") +
    theme_light()  +
    theme(plot.title = element_text(face = "italic"), legend.position = "bottom")
```
![](images/acanthurus.png)

You can also take advantage of the H3 system to aggregate information in cells.

```r
acanthurus_agg <- acanthurus %>%
    sf::st_drop_geometry() %>%
    mutate(h3_4 = h3jsr::get_parent(h3_7, res = 4)) %>%
    group_by(h3_4) %>%
    summarise(mean_sst = mean(coraltempSST))

acanthurus_agg_pol <- h3jsr::cell_to_polygon(acanthurus_agg$h3_4)
acanthurus_agg_pol <- st_as_sf(acanthurus_agg_pol)
acanthurus_agg_pol <- bind_cols(acanthurus_agg_pol, acanthurus_agg)

ggplot() +
    geom_sf(data = rnaturalearth::ne_countries(returnclass = "sf"),
            color = "gray80", fill = "gray80") +
    geom_sf(data = acanthurus_agg_pol, aes(fill = mean_sst)) +
    scale_fill_viridis_c() +
    coord_sf(xlim = c(-95, -30), ylim = c(-30, 30)) +
    ggtitle("Acanthurus choeruleus", "Year: 2014 - Product: CoralTemp (NOAA)") +
    theme_light()  +
    theme(plot.title = element_text(face = "italic"), legend.position = "bottom")
```
![](images/acanthurus_agg.png)

### Python

Static map plot:

``` python
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import contextily as ctx
from palettable.matplotlib import Viridis_10

species_filter = [("species", "==", "Acanthurus chirurgus")]
year_filter = [("year", "==", 2000)]

gdf = gpd.read_parquet("aggregated/", filters=species_filter + year_filter)[["geometry", "species", "coraltempSST", "year"]]

values = gdf["coraltempSST"]
norm = (values - values.min()) / (values.max() - values.min())

cmap = Viridis_10.mpl_colormap  # Sequential colormap
norm = mcolors.Normalize(vmin=values.min(), vmax=values.max())

gdf = gdf.to_crs(epsg=3857)

fig, ax = plt.subplots(figsize=(10, 8))
sc = gdf.plot(column="coraltempSST", 
              cmap=cmap, 
              norm=norm, 
              markersize=5, 
              legend=False, 
              ax=ax, 
              alpha=0.7)

ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, zoom=5)

sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm.set_array([]) 
cbar = fig.colorbar(sm, ax=ax, orientation="vertical")
cbar.set_label("Coral Temperature SST (°C)")

ax.set_xticks([])
ax.set_yticks([])
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.set_title("Species: Acanthurus chirurgus - Coral Temperature SST")

plt.show()
```
![](images/species_py.png)

Dynamic map with Lonboard:

``` python
import geopandas as gpd
import lonboard
from lonboard.colormap import apply_continuous_cmap
import seaborn as sns
import pandas as pd
from palettable.colorbrewer.diverging import BrBG_10

species_filter = [("species", "==", "Acanthurus chirurgus")]
year_filter = [("year", "==", 2000)]

gdf = gpd.read_parquet("aggregated/", filters=species_filter + year_filter)[["geometry", "species", "coraltempSST", "year"]]

point_layer = lonboard.ScatterplotLayer.from_geopandas(gdf)

values = gdf["coraltempSST"]
normalized_values = (values - values.min()) / (values.max() - values.min())

point_layer.get_radius = 10000
point_layer.radius_max_pixels = 2
point_layer.get_fill_color = apply_continuous_cmap(normalized_values, BrBG_10, alpha=0.7)

Map(point_layer)
```

![](images/lonboard_py.png)

## Notebooks

Check out other examples of use in our [notebooks](https://github.com/iobis/obis-therm/tree/main/notebooks).

## Updates

- The code now uses `Dask` integration with `xarray` for parallel processing.

## Next steps

- Convert all main code to Python, and just do post-processing on R.