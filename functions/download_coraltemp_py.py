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

    lats = xr.DataArray(target_data['decimalLatitude'], dims = "z")
    lons = xr.DataArray(target_data['decimalLongitude'], dims = "z")

    selected_data = dataset['sea_surface_temperature'].sel(
            **{lon_var: lons, 
               lat_var: lats,
               'time': target_date},
            method='nearest'
        )

    result_df = selected_data.to_dataframe()

    result_df.rename(columns={'time': 'actual_date', 'sea_surface_temperature': 'value'}, inplace=True)
    result_df['requested_date'] = target_date
    result_df['temp_ID'] = target_data['temp_ID']
    result_df = result_df[['temp_ID', 'requested_date', 'actual_date', 'value']]

    return result_df
