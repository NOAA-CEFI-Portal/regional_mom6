#!/usr/bin/env python

"""
change datetime encoding to int32
add attribute for reference 
"""

import glob
import xarray as xr


indirs = [
    '/Datasets.private/regional_mom6/northwest_atlantic/forecast/',
    '/Datasets.private/regional_mom6/northwest_atlantic/forecast/regrid/'
]
outdirs = [
    '/Projects/CEFI/regional_mom6/northwest_atlantic/forecast/',
    '/Projects/CEFI/regional_mom6/northwest_atlantic/forecast/regrid/'
]

for ndir, indir in enumerate(indirs):
    file_list = glob.glob(f'{indir}*.nc')
    for file in file_list :
        print('----')
        print(file)
        ds = xr.open_dataset(file)
        filename = file[len(indir):]
        ds.init.encoding["dtype"] = 'int32'

        ds.attrs['paper_reference'] = 'https://doi.org/10.5194/egusphere-2024-394'
        ds.attrs['data_reference'] = 'https://doi.org/10.5281/zenodo.10642294'

        print(f'{outdirs[ndir]}{filename}')
        ds.to_netcdf(f'{outdirs[ndir]}{filename}')
