for **years**
    for **months**
        if (year AND month IN "glorys" range)
            1. Open correct dataset according to month/year
            2. Download full data for month/year
            3. Open dataset using `xarray` connection to Copernicus Marine Store
            4. Prepare working dataset (`obis_dataset`)
            5. Verify which coordinates are NA and try to assign nearest valid cell
            6. Mark which coordinates are still not valid and put placeholders
            7. Get valid maximum and minimum depths (function `get_depths`)
            8. See which are not valid, put placeholders and mark for later removal
            9. Call function `download_temp` with open dataset (i.e., no download again) and try to extract data
                if (STEP 9 succeeds)
                    a. Input downloaded data in the final dataset (`all_vals`)
                    b. Set lines that are not valid (coordinates or depths) as NA
                    c. Tag lines that were approximated for coordinates
                    d. Tag lines that were approximated for depth min or max
                else
                    a. Mark download as failed for this year/month
        if (year AND month IN "coraltemp" range)
            1. Get valid cells using downloaded sample and try to assign nearest valid cell
            2. Mark which coordinates are still not valid and put placeholders
            3. Download data using OPeNDAP (through `xarray`)
                if (STEP 2 succeeds)
                    a. Input downloaded data in the final dataset (`all_vals`)
                    b. Set lines that are not valid (coordinates) as NA
                    c. Tag lines that were approximated for coordinates
                else
                    a. Mark download as failed for this year/month
        if (year AND month IN "mur" range)
            1. Get valid cells using downloaded sample and try to assign nearest valid cell
            2. Mark which coordinates are still not valid and put placeholders
            3. Download data using OPeNDAP (through `xarray`)
                if (STEP 2 succeeds)
                    a. Input downloaded data in the final dataset (`all_vals`)
                    b. Set lines that are not valid (coordinates) as NA
                    c. Tag lines that were approximated for coordinates
                else
                    a. Mark download as failed for this year/month
        if (year AND month IN "ostia" range)
            1. Get valid cells using downloaded sample and try to assign nearest valid cell
            2. Mark which coordinates are still not valid and put placeholders
            3. Download data using `xarray` connection to Copernicus Marine Store
                if (STEP 2 succeeds)
                    a. Input downloaded data in the final dataset (`all_vals`)
                    b. Set lines that are not valid (coordinates) as NA
                    c. Tag lines that were approximated for coordinates
                else
                    a. Mark download as failed for this year/month
        1. Join values object with original dataset (OBIS) for this month/year
        2. Remove lines with no valid value for any of the products
        3. Save object as a parquet file tagged by year AND month
1. Add H3 indexing at resolution 7
2. Convert individual files to GeoParquet format
3. Export final dataset to S3 bucket

To update:
for **years**
    for **months**
        1. Open previous dataset
        2. Open OBIS data
        3. Check if there are new data
        if NEW DATA
            a. Proceeds with all steps
            b. Save new data with PREVIOUS dataset and NEW DATASET joined
        else: keep old dataset for that year/month