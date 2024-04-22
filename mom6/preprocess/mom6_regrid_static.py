"""
This script is designed to do regridding of the field in the static file
regional mom6

The regridding is using the xesmf package with the bilinear method
there are other more conservative way to doing the regriddeding 
https://xesmf.readthedocs.io/en/stable/notebooks/Compare_algorithms.html 

Current regridded product is based on the following keys
1. The number of grid is close or similar to the original 
   curvilinear grid.
2. The regridded product should cover the maximum/minimum lon lat
   range that is provided by the original curvilinear grid

Based on the above mentioned keys
1. the number of grid in lon,lat is the same as xh,yh
2. the linspace range of lon lat is defined by 
   max(geolon/geolat) and min(geolon/geolat)

"""
# %%
import os
import warnings
import numpy as np
import xarray as xr
import xesmf as xe
from dask.distributed import Client
from mom6 import DATA_PATH
from mom6.mom6_module.mom6_io import MOM6Misc
warnings.simplefilter("ignore")

regrid_var = 'wet'

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # %%
    # file location and name
    mom6_static_dir = os.path.join(DATA_PATH,"static")

    # %%
    # static field
    ds_static = xr.open_dataset(f'{mom6_static_dir}ocean_static.nc')
    ds_static = ds_static.set_coords(
        ['geolon','geolat',
         'geolon_c','geolat_c',
         'geolon_u','geolat_u',
         'geolon_v','geolat_v']
    )
    ds_static['geolon'] = ds_static['geolon']+360.
    # for xesmf to rename
    ds_static = ds_static.rename({'geolon':'lon','geolat':'lat'})

    # %%
    # Create longitude and latitude arrays (e.g., 1D arrays)
    x = np.linspace(
        ds_static.lon.min(),
        ds_static.lon.max(),
        len(ds_static.xh)-1
    )    # Example longitude values
    y = np.linspace(
        ds_static.lat.min(),
        ds_static.lat.max(),
        len(ds_static.yh)-1
    )    # Example latitude values

    # Create a dummy data variable (optional, can be empty)
    data = xr.DataArray(
        data=None,
        coords={'lon': x, 'lat': y},
        dims=('lon', 'lat')
    )

    # Create an xarray dataset with empty dataarray
    ds_regrid = xr.Dataset({'var': data})

    # %%
    # use xesmf to create regridder
    # !!!! regridded only suited for geolon and geolat to x and y
    regridder = xe.Regridder(ds_static, ds_regrid, "bilinear", unmapped_to_nan=True)

    # %%
    # open each file and regrid
    ds_regrid = xr.Dataset({regrid_var: data})

    # perform regrid for each field
    ds_regrid[regrid_var] = regridder(ds_static[regrid_var])

    # output the netcdf file
    print(f'output {mom6_static_dir}/regrid/ocean_static.{regrid_var}.nc')
    MOM6Misc.mom6_encoding_attr(
            ds_static,
            ds_regrid,
            var_names=[regrid_var],
            dataset_name='regional mom6 regrid'
        )
    ds_regrid.to_netcdf(f'{mom6_static_dir}/regrid/ocean_static.{regrid_var}.nc')
