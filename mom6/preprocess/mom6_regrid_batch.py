"""
This script is designed to do batch regridding of the 
regional mom6 output

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
import sys
import glob
import warnings
import numpy as np
import xarray as xr
from mom6 import DATA_PATH
from mom6.mom6_module.mom6_io import MOM6Static,MOM6Misc
warnings.simplefilter("ignore")

# %%
if __name__=="__main__":
    import xesmf as xe
    from dask.distributed import Client

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # check argument exist
    if len(sys.argv) < 2:
        print("Usage: python mom6_regrid.py hist/fcst")
        sys.exit(1)
    # %%
    if sys.argv[1] == 'hist':
        # file location and name
        # mom6_dir = "/Datasets.private/regional_mom6/hist_run/"
        mom6_dir = os.path.join(DATA_PATH,"hist_run/")
        file_list = glob.glob(f'{mom6_dir}/*.nc')
    elif sys.argv[1] == 'fcst':
        # mom6_dir = "/Datasets.private/regional_mom6/forecast/"
        mom6_dir = os.path.join(DATA_PATH,"forecast/")
        file_list = glob.glob(f'{mom6_dir}/*.nc')

    # %%
    # static field
    ds_static = MOM6Static.get_grid(DATA_PATH+'static/')
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
    ds = xr.open_dataset(file_list[0],chunks={})
    if sys.argv[1] == 'hist':
        data = xr.DataArray(
            data=None,
            coords={'lon': x, 'lat': y},
            dims=('lon', 'lat')
        )
    elif sys.argv[1] == 'fcst':
        data = xr.DataArray(
            data=None,
            coords={'lon': x, 'lat': y, "member": ds.member, "init": ds.init, "lead": ds.lead},
            dims=('lon', 'lat', 'member', 'init', 'lead')
        )

    # Create an xarray dataset with empty dataarray
    ds_regrid = xr.Dataset({'var': data})

    # %%
    # use xesmf to create regridder
    # !!!! regridded only suited for geolon and geolat to x and y
    regridder = xe.Regridder(ds_static, ds_regrid, "bilinear", unmapped_to_nan=True)

    # %%
    # open each file and regrid
    for file in file_list :
        ds = xr.open_dataset(file)

        if "siconc" in file:
            # cover dim name to the corrected name for Tpoint(error in the model)
            ds = ds.rename({'xT':'xh','yT':'yh'})

        # merge with static field
        ds = xr.merge([ds,ds_static])

        if sys.argv[1] == 'hist':
            varname = file.split(".")[-2]
            ds_regrid = xr.Dataset()
            # perform regrid for each field
            ds_regrid[varname] = regridder(ds[varname])

        elif sys.argv[1] == 'fcst':
            varname = file.split("/")[-1].split("_")[0]
            ds_regrid = xr.Dataset()
            # perform regrid for each field
            ds_regrid[varname] = regridder(ds[varname])
            ds_regrid[f"{varname}_anom"] = regridder(ds[f"{varname}_anom"])

        # output the netcdf file
        print(f'output {mom6_dir}/regrid/{file[len(mom6_dir):]}')
        MOM6Misc.mom6_encoding_attr(
                ds,
                ds_regrid,
                var_names=list(ds_regrid.keys()),
                dataset_name='regional mom6 regrid'
            )
        try:
            ds_regrid.to_netcdf(f'{mom6_dir}/regrid/{file[len(mom6_dir):]}',mode='w')
        except PermissionError:
            print(f'{mom6_dir}/regrid/{file[len(mom6_dir):]} is used by other scripts' )
