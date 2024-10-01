from functions.sort_dimension import sort_dimension

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

    coord_names = list(dataset.coords)

    lat_var = [coord for coord in coord_names if coord.startswith('lat')][0]
    lon_var = [coord for coord in coord_names if coord.startswith('lon')][0]

    dataset = sort_dimension(dataset, lat_var)
    dataset = sort_dimension(dataset, lon_var)
    
    target_date = pd.to_datetime(sel_date)
    
    ds = dataset['sea_surface_temperature'].sel(
        time=target_date, method='nearest'
    )

    results = []    

    # Iterate over each row in the DataFrame
    for i, row in target_data.iterrows():
        
        selected_data = ds.sel(
            **{lon_var: row['decimalLongitude'], 
               lat_var: row['decimalLatitude']},
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
