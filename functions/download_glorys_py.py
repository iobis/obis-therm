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

    dataset = sort_dimension(dataset, 'latitude')
    dataset = sort_dimension(dataset, 'longitude')

    lons = xr.DataArray(target_data['decimalLongitude'], dims="z")
    lats = xr.DataArray(target_data['decimalLatitude'], dims="z")

    target_date = pd.to_datetime(sel_date)

    depth_columns = ['depth_surface', 'depth_mid', 'depth_deep', 'depth_min', 'depth_max']
    results = []

    for depth_col in depth_columns:

        depth_coord = xr.DataArray(target_data[depth_col], dims="z")

        selected_data = dataset['thetao'].sel( 
                longitude = lons,
                latitude = lats,
                depth = depth_coord, 
                time = target_date,
                method = 'nearest'
            )
        
        df = selected_data.to_dataframe().rename(columns={'thetao': 'value'}).reset_index()
        df['depth_type'] = depth_col
        df['temp_ID'] = target_data['temp_ID']

        results.append(df)

    # Extract bottom temperature
    selected_data_bottom = dataset['bottomT'].sel( 
            longitude = lons,
            latitude = lats,
            time = target_date,
            method='nearest'
        )
    
    df = selected_data_bottom.to_dataframe().rename(columns={'bottomT': 'value'}).reset_index()
    df['depth_type'] = 'depth_bottom'
    df['temp_ID'] = target_data['temp_ID']

    results.append(df)

    pd.concat(results, axis=1).reset_index()

    result_df = pd.concat(results, ignore_index=True)

    return result_df
