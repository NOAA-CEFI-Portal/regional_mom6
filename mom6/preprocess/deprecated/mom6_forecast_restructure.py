#!/usr/bin/env python

"""
Restructure the hindcast/forecast data to 
one initialization one file.
"""

import glob
import xarray as xr
from mom6 import DATA_PATH

# %%
file_list = glob.glob(f'{DATA_PATH}hindcast/regrid/*.nc')

# %%
for file in file_list :
    ds = xr.open_dataset(file)
    coords = list(ds.coords)
    variables = [var for var in list(ds.variables) if var not in coords]

    for i in range(len(ds.init)):
        year = ds.isel(init=i)['init.year'].data
        month = ds.isel(init=i)['init.month'].data
        ds.isel(init=i).to_netcdf(
            f'{DATA_PATH}forecast/regrid/{variables[0]}_forecast_i{year:04d}{month:02d}.nc'
        )
