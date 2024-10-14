from functions.sort_dimension import sort_dimension

def download_glorys_py(dataset, target_data, sel_date):
    """
    Download data from GLORYS product.

    Args:
        dataset: a dataset opened through the Copernicus Marine API (copernicusmarine).
        target_data: the target dataset containing columns `decimalLongitude`, `decimalLatitude`,
                    `temp_ID`, `depth_surface`, `depth_mid`, `depth_deep`, `depth_min` and `depth_max`.
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

    # Define the depth columns
    depth_columns = ['depth_surface', 'depth_mid', 'depth_deep', 'depth_min', 'depth_max']

    results = []

    target_date = pd.to_datetime(sel_date)

    # Iterate over each row in the DataFrame
    for i, row in target_data.iterrows():
        for depth_col in depth_columns:
            depth = row[depth_col]
        
            selected_data = dataset['thetao'].sel(
                longitude=row['decimalLongitude'], 
                latitude=row['decimalLatitude'], 
                depth=depth, 
                time=target_date,
                method='nearest'
            )
        
            # Get the actual depth and time that was used in the selection
            actual_depth = selected_data['depth'].item()
            actual_time = pd.to_datetime(selected_data['time'].item())

            sst_value = selected_data.item()

            # Append the results
            results.append({
                'temp_ID': int(row['temp_ID']),
                #'decimalLongitude': row['decimalLongitude'],
                #'decimalLatitude': row['decimalLatitude'],
                'actual_lon': selected_data['longitude'].item(),
                'actual_lat': selected_data['latitude'].item(),
                'requested_depth': depth,
                'actual_depth': actual_depth,
                'depth_type': depth_col,
                'requested_date': target_date,
                'actual_date': actual_time,
                'value': sst_value,
            })
        
        # Extract bottom temperature
        selected_data_bottom = dataset['bottomT'].sel(
            longitude=row['decimalLongitude'], 
            latitude=row['decimalLatitude'],
            time=target_date,
            method='nearest'
        )

        actual_time = pd.to_datetime(selected_data_bottom['time'].item())
        sst_value = selected_data_bottom.item()

        # Append the results
        results.append({
            'temp_ID': int(row['temp_ID']),
            #'decimalLongitude': row['decimalLongitude'],
            #'decimalLatitude': row['decimalLatitude'],
            'requested_depth': None,
            'actual_depth': None,
            'depth_type': 'depth_bottom',
            'requested_date': target_date,
            'actual_date': actual_time,
            'value': sst_value,
        })

    result_df = pd.DataFrame(results)

    return result_df
