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

def get_glorys_depth(dataset, target_data, sel_date):
    """
    Get maximum and minimum depth from GLORYS

    Args:
        dataset: a dataset opened through the Copernicus Marine API (copernicusmarine).
        target_data: the target dataset containing columns `decimalLongitude`, `decimalLatitude`,
                    `temp_ID`
        sel_date: selected date

    Returns:
        data frame: The data frame with the requested data.

    Depends:
        xarray, pandas
    """
    
    import numpy as np
    import pandas as pd

    dataset = sort_dimension(dataset, 'latitude')
    dataset = sort_dimension(dataset, 'longitude')

    target_date = pd.to_datetime(sel_date)

    ds = dataset.sel(
        time = target_date,
        method = 'nearest'
    )

    def get_max_mid_depth(lon, lat, ds):
        data_point = ds.sel(longitude=lon, latitude=lat, method='nearest')

        depths = data_point.depth.values
        data_values = data_point['thetao'].values

        valid_depths = depths[~np.isnan(data_values)]
    
        if len(valid_depths) > 0:
            max_depth = valid_depths.max()
            surface_depth = valid_depths.min()
            mid_value = (max_depth + surface_depth) / 2
            closest_mid_depth = valid_depths[np.argmin(np.abs(valid_depths - mid_value))]
        
            return max_depth, closest_mid_depth
        else:
            return np.nan, np.nan

    # Apply the function to each row in the points dataframe
    target_data[['max_depth', 'mid_depth']] = target_data.apply(
        lambda row: get_max_mid_depth(row['decimalLongitude'], row['decimalLatitude'], ds), axis=1, result_type='expand'
    )

    return(target_data)