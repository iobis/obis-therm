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