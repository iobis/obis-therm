def sort_dimension(dataset, dim_name):
    """
    Get the values for the specified dimension and verify if they are unsorted. If so, the function sorts them.

    Source: https://help.marine.copernicus.eu/en/articles/7970637-how-to-download-data-for-multiple-points-from-a-csv
    """
    # Get the coordinate values for the specified dimension.
    coordinates = dataset[dim_name].values

    # Check if the coordinates are unsorted.
    if (coordinates[0] >= coordinates[:-1]).all():
        dataset = dataset.sortby(dim_name, ascending=True)
        
    return dataset

def download_coraltemp_py(dataset, target_data, sel_date):
    """
    Download data from CoralTemp product (NOAA).

    Args:
        dataset: a dataset opened through xarray (example: `xr.open_dataset("https://coastwatch.pfeg.noaa.gov/erddap/griddap/NOAA_DHW_monthly")`).
        target_data: the target dataset containing columns `decimalLongitude`, `decimalLatitude`,
                    and `temp_ID`.
        sel_date: selected date

    Returns:
        data frame: The data frame with the requested data.

    Depends:
        xarray, pandas
    """
    
    import xarray as xr
    import pandas as pd

    dataset = sort_dimension(dataset, 'latitude')
    dataset = sort_dimension(dataset, 'longitude')

    results = []

    target_date = pd.to_datetime(sel_date)

    # Iterate over each row in the DataFrame
    for i, row in target_data.iterrows():
        
        selected_data = dataset['sea_surface_temperature'].sel(
            longitude=row['decimalLongitude'], 
            latitude=row['decimalLatitude'],
            time=target_date,
            method='nearest'
        )

        actual_time = pd.to_datetime(selected_data['time'].item())
        sst_value = selected_data.item()

        # Append the results
        results.append({
            'temp_ID': int(row['temp_ID']),
            #'decimalLongitude': row['decimalLongitude'],
            #'decimalLatitude': row['decimalLatitude'],
            'requested_date': target_date,
            'actual_date': actual_time,
            'value': sst_value,
        })

    result_df = pd.DataFrame(results)

    return result_df
