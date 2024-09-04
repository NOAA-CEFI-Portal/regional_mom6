#!/usr/bin/env python

"""
change datetime encoding to int32
add attribute for reference 
"""

import glob
import xarray as xr


indirs = [
    '/Datasets.private/regional_mom6/northwest_atlantic/hist_run/regrid/'
]
outdirs = [
    '/Projects/CEFI/regional_mom6/northwest_atlantic/hist_run/regrid/'
]

for ndir, indir in enumerate(indirs):
    file_list = glob.glob(f'{indir}*.nc')
    for file in file_list :
        print('----')
        print(file)
        filename = file[len(indir):]
        ds = xr.open_dataset(file)
        try:
            if ds.isel(time=0)['time.year'] == 1980 :
                ds = ds.isel(time=slice(1,None))

                print(f'{outdirs[ndir]}{filename}')
                ds.to_netcdf(f'{outdirs[ndir]}{filename}')
        except ValueError:
            print('No time dimension')
            pass
