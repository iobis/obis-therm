from functions.sort_dimension import sort_dimension

def download_glorys_py(dataset, target_data, sel_date, verbose = True):
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
    from progress.bar import Bar

    dataset = sort_dimension(dataset, 'latitude')
    dataset = sort_dimension(dataset, 'longitude')

    lons = xr.DataArray(target_data['decimalLongitude'], dims="z")
    lats = xr.DataArray(target_data['decimalLatitude'], dims="z")

    temp_res = dataset.sel(latitude=lats, longitude=lons, method="nearest")

    x_indices = dataset.longitude.get_index("longitude").get_indexer(temp_res.longitude)
    y_indices = dataset.latitude.get_index("latitude").get_indexer(temp_res.latitude)

    target_data['X_index'] = x_indices
    target_data['Y_index'] = y_indices

    target_data['unique_ID'] = target_data.groupby(['X_index', 'Y_index', 'depth_surface', 'depth_mid', 'depth_deep', 'depth_min', 'depth_max']).ngroup()

    unique_groups_df = target_data[['X_index', 'Y_index', 'unique_ID', 'depth_surface', 'depth_mid', 'depth_deep', 'depth_min', 'depth_max']].drop_duplicates()
    
    # Define the depth columns
    depth_columns = ['depth_surface', 'depth_mid', 'depth_deep', 'depth_min', 'depth_max']

    results = []

    target_date = pd.to_datetime(sel_date)

    if verbose:
        bar = Bar('Retrieving data', max=len(unique_groups_df))

    # Iterate over each row in the DataFrame
    for i, row in unique_groups_df.iterrows():
        for depth_col in depth_columns:
            depth = row[depth_col]
        
            selected_data = dataset['thetao'].isel(
                longitude=row['X_index'].astype(int), 
                latitude=row['Y_index'].astype(int)).sel( 
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
                'unique_ID': int(row['unique_ID']),
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
        selected_data_bottom = dataset['bottomT'].isel(
            longitude=row['X_index'].astype(int), 
            latitude=row['Y_index'].astype(int)).sel(
            time=target_date,
            method='nearest'
        )

        actual_time = pd.to_datetime(selected_data_bottom['time'].item())
        sst_value = selected_data_bottom.item()

        # Append the results
        results.append({
            'unique_ID': int(row['unique_ID']),
            #'decimalLongitude': row['decimalLongitude'],
            #'decimalLatitude': row['decimalLatitude'],
            'requested_depth': None,
            'actual_depth': None,
            'depth_type': 'depth_bottom',
            'requested_date': target_date,
            'actual_date': actual_time,
            'value': sst_value,
        })
        if verbose:
            bar.next()
    if verbose:
        bar.finish()

    results = pd.DataFrame(results)
    merged_df = pd.merge(target_data, results, on='unique_ID', how='left')

    result_df = merged_df.drop(columns=['X_index', 'Y_index', 'unique_ID'])

    return result_df
