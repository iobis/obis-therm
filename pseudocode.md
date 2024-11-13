for **years**  
─ for **months**  
─── **if (year AND month IN "glorys" range)**  
─────  1. Open correct dataset according to month/year  
─────  2. if (number of points is less than 100) Connect through `xarray` directly to the Copernicus Marine Store, else, Download full data for month/year   
─────  3. Open dataset using `xarray`  
─────  4. Verify which coordinates are NA and try to assign nearest valid cell    
─────  5. Mark which coordinates are still not valid and put placeholders    
─────  6. Get valid maximum and minimum depths (function `get_depths`)  
─────  7. See which are not valid, put placeholders and mark for later removal  
─────  9. Call function `download_temp` with open dataset (i.e., no download again) and try to extract data   
──────  if (STEP 9 succeeds)  
───────  a. Input downloaded data in the final dataset (`all_vals`)  
───────  b. Set lines that are not valid (coordinates or depths) as NA   
───────  c. Tag lines that were approximated for coordinates  
───────  d. Tag lines that were approximated for depth min or max   
──────  else   
───────  a. Mark download as failed for this year/month   
── **if (year AND month IN "coraltemp" range)**   
───  1. if (number of points is less than 100) Connect through `xarray` directly to the OPeNDAP, else, Download full data for month/year  
───  2. Open dataset using `xarray`  
───  3. Verify which coordinates are NA and try to assign nearest valid cell    
───  4. Mark which coordinates are still not valid and put placeholders   
───  5. Call function `download_temp` with open dataset and try to extract data   
────  if (STEP 5 succeeds)   
─────  a. Input downloaded data in the final dataset (`all_vals`)   
─────  b. Set lines that are not valid (coordinates) as NA   
─────  c. Tag lines that were approximated for coordinates   
────  else   
─────  a. Mark download as failed for this year/month   
── **if (year AND month IN "mur" range)**   
───  1. if (number of points is less than 100) Connect through `xarray` directly to the OPeNDAP, else, Download full data for month/year  
───  2. Open dataset using `xarray`  
───  3. Verify which coordinates are NA and try to assign nearest valid cell    
───  4. Mark which coordinates are still not valid and put placeholders   
───  5. Call function `download_temp` with open dataset and try to extract data   
────  if (STEP 5 succeeds)   
─────  a. Input downloaded data in the final dataset (`all_vals`)   
─────  b. Set lines that are not valid (coordinates) as NA   
─────  c. Tag lines that were approximated for coordinates   
────  else   
─────  a. Mark download as failed for this year/month   
── **if (year AND month IN "ostia" range)**   
───  1. if (number of points is less than 100) Connect through `xarray` directly to the Copernicus Marine Store, else, Download full data for month/year  
───  2. Open dataset using `xarray`  
───  3. Verify which coordinates are NA and try to assign nearest valid cell    
───  4. Mark which coordinates are still not valid and put placeholders   
───  5. Call function `download_temp` with open dataset and try to extract data   
────  if (STEP 5 succeeds)   
─────  a. Input downloaded data in the final dataset (`all_vals`)   
─────  b. Set lines that are not valid (coordinates) as NA   
─────  c. Tag lines that were approximated for coordinates   
────  else   
─────  a. Mark download as failed for this year/month   
1. Join values object with original dataset (OBIS) for this month/year  
2. Remove lines with no valid value for any of the products  
3. Save object as a parquet file tagged by year AND month  

Aggregate files:  
1. Add H3 indexing at resolution 7  
2. Convert individual files to GeoParquet format  
3. Export final dataset to S3 bucket  