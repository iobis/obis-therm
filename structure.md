# Dataset structure

The dataset is structured as this:

|column                   |class        |
|:------------------------|:------------|
|AphiaID                  |integer      |
|species                  |character    |
|family                   |character    |
|id                       |character    |
|dataset_id               |character    |
|occurrenceID             |character    |
|datasetID                |character    |
|minimumDepthInMeters     |numeric      |
|maximumDepthInMeters     |numeric      |
|decimalLongitude         |numeric      |
|decimalLatitude          |numeric      |
|eventDate                |character    |
|date_mid                 |integer64    |
|month                    |integer      |
|surfaceTemperature       |numeric      |
|midTemperature           |logical      |
|deepTemperature          |logical      |
|bottomTemperature        |numeric      |
|midDepth                 |logical      |
|deepDepth                |logical      |
|minimumDepthTemperature  |numeric      |
|maximumDepthTemperature  |numeric      |
|minimumDepthClosestDepth |numeric      |
|maximumDepthClosestDepth |numeric      |
|coraltempSST             |numeric      |
|murSST                   |numeric      |
|ostiaSST                 |numeric      |
|flag                     |integer      |
|medianTemperature        |numeric      |
|medianDepth              |numeric      |
|bottomDepth              |numeric      |
|geometry                 |arrow_binary |
|h3_7                     |character    |
|year                     |integer      |

The columns `id` and `dataset_id` enable you to link and join this dataset with the OBIS database.

The `geometry` column is a binary geometry format (see more about the GeoParquet [format here](https://geoparquet.org/)). The `h3_7` column contains the H3 grid code at the resolution 7. You can use this to easily aggregate data. Because Uber's H3 system is hierarchical, you can also aggregate in coarser resolutions. See more about the H3 system [here](https://h3geo.org/) and the resolutions table [here](https://h3geo.org/docs/core-library/restable).

## Temperature columns

* `surfaceTemperature`: this is the GLORYS surface temperature
* `midTemperature`: this is the GLORYS temperature for the mid depth (that is, the mid point between the maximum depth with valid values and the surface depth)
* `deepTemperature`: this is the GLORYS temperature for the maximum depth with valid values
* `bottomTemperature`: this is the GLORYS temperature for the bottom (i.e. the 'Sea water potential temperature at sea floor' variable

For both `midTemperature` and `deepTemperature` you should look at `midDepth` and `deepDepth` to see which is the depth used.

* `minimumDepthTemperature`: this is the GLORYS temperature for the depth recorded on the column `minimumDepthInMeters`, that is, the minimum depth given by the original data
* `maximumDepthTemperature`: this is the GLORYS temperature for the depth recorded on the column `maximumDepthInMeters`, that is, the maximum depth given by the original data

For both `minimumDepthTemperature` and `maximumDepthTemperature` you should look at `minimumDepthClosestDepth` and `maximumDepthClosestDepth` to see which is the depth that was actually used.

* `coraltempSST`: SST according to the CoralTemp
* `murSST`: SST according to the MUR
* `ostiaSST`: SST according to the OSTIA

## Flags

* 0 = no problem identified  
* 1 = date is range (i.e. the date_start and date_end are different). Note that the date that is used to retrieve the data is the date_mid/date_year column  
* 2 = GLORYS coordinate is approximated (i.e., the target cell had no value - was NA - and we searched for the nearest valid point in the 25 nearest cells)  
* 4 = Minimum depth closest value is more than 5 meters different than the true value  
* 8 = Maximum depth closest value is more than 5 meters different than the true value  
* 16 = CoralTempSST coordinate is approximated   
* 32 = MUR SST coordinate is approximated  
* 64 = OSTIA SST coordinate is approximated  

Flags can be summed. E.g. a flag = 6 means that date is range and surfaceTemperature and medianTemperature are approximated.

## Sample of the dataset

| AphiaID|species             |family        |id                                   |dataset_id                           |occurrenceID                                              |datasetID                              | minimumDepthInMeters| maximumDepthInMeters| decimalLongitude| decimalLatitude|eventDate                 |     date_mid| month| surfaceTemperature|midTemperature |deepTemperature | bottomTemperature|midDepth |deepDepth | minimumDepthTemperature| maximumDepthTemperature| minimumDepthClosestDepth| maximumDepthClosestDepth| coraltempSST| murSST| ostiaSST| flag| medianTemperature| medianDepth| bottomDepth|h3_7            | year|
|-------:|:-------------------|:-------------|:------------------------------------|:------------------------------------|:---------------------------------------------------------|:--------------------------------------|--------------------:|--------------------:|----------------:|---------------:|:-------------------------|------------:|-----:|------------------:|:--------------|:---------------|-----------------:|:--------|:---------|-----------------------:|-----------------------:|------------------------:|------------------------:|------------:|------:|--------:|----:|-----------------:|-----------:|-----------:|:---------------|----:|
|  104499|Centropages typicus |Centropagidae |52f988b5-cacf-4e4e-9d11-29288e3e1c4e |e981eab6-f849-4891-8fac-495852829456 |urn:catalog:MBA:CPR:325SB-37-6                            |https://marineinfo.org/id/dataset/216  |                    5|                   10|          -9.2400|          39.920|1986-12-12T04:48:00+00:00 | 534729600000|    12|                 NA|NA             |NA              |                NA|NA       |NA        |                      NA|                      NA|                       NA|                       NA|        14.83|     NA|       NA|    0|                NA|          NA|          NA|873931464ffffff | 1986|
|  112100|NA                  |Cancrisidae   |7a45ded4-99c0-4043-9a35-050e8e04fe68 |b58d5ca0-1af8-48b3-993d-6c89742bb0d2 |urn:catalog:Pangaea:doi:10.1594/PANGAEA.745661:11180055_4 |https://marineinfo.org/id/dataset/2756 |                  757|                   NA|         -78.9442|         -11.538|1986-12-10                | 534556800000|    12|                 NA|NA             |NA              |                NA|NA       |NA        |                      NA|                      NA|                       NA|                       NA|        21.57|     NA|       NA|    0|                NA|          NA|          NA|878e746e0ffffff | 1986|
|  112100|NA                  |Cancrisidae   |2b9418e5-20ea-4ac7-b012-b2afcbd1f25a |b58d5ca0-1af8-48b3-993d-6c89742bb0d2 |urn:catalog:Pangaea:doi:10.1594/PANGAEA.745661:11180055_2 |https://marineinfo.org/id/dataset/2756 |                  681|                   NA|         -78.9442|         -11.538|1986-12-10                | 534556800000|    12|                 NA|NA             |NA              |                NA|NA       |NA        |                      NA|                      NA|                       NA|                       NA|        21.57|     NA|       NA|    0|                NA|          NA|          NA|878e746e0ffffff | 1986|
|  104499|Centropages typicus |Centropagidae |652adedd-b221-4510-ba8c-05e954d558e8 |e981eab6-f849-4891-8fac-495852829456 |urn:catalog:MBA:CPR:325SB-19-6                            |https://marineinfo.org/id/dataset/216  |                    5|                   10|          -9.4183|          42.795|1986-12-10T03:11:00+00:00 | 534556800000|    12|                 NA|NA             |NA              |                NA|NA       |NA        |                      NA|                      NA|                       NA|                       NA|        13.96|     NA|       NA|    0|                NA|          NA|          NA|87392445bffffff | 1986|
|  149054|NA                  |Paraliaceae   |428ac009-c3fa-43e5-86bf-682211df538e |14408cc8-5d37-46d1-91a9-d838fc339ff9 |imos_apd_data:P509_251519861228_824                       |NA                                     |                   NA|                   NA|         141.5000|         -12.500|1986-12-28T00:00:00Z      | 536112000000|    12|                 NA|NA             |NA              |                NA|NA       |NA        |                      NA|                      NA|                       NA|                       NA|        30.19|     NA|       NA|    0|                NA|          NA|          NA|879ce0824ffffff | 1986|

Note: in the above table, we removed the `geometry` column.
