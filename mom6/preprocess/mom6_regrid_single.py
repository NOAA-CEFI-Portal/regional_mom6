"""
This script is designed to do specified regridding of the 
regional mom6 output

!!!
current implementation only suitable for geolon geolat (tracer points)
!!!

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
import warnings
import numpy as np
import xarray as xr
import xesmf as xe
from dask.distributed import Client
from mom6.mom6_module.deprecated.mom6_io import MOM6Misc,MOM6Historical,MOM6Forecast

warnings.simplefilter("ignore")

if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # check argument exist
    if len(sys.argv) < 2:
        print("Usage: python mom6_regrid_single.py <hist/fcst> <varname> <freq>")
        sys.exit(1)

    # dictionary for input argument to dir name
    dict_dir = {
        'hist': 'hist_run/',
        'fcst': 'forecast/'
    }

    data_type = sys.argv[1]
    varname = sys.argv[2]

    # for historical run
    if data_type == 'hist':
        freq = sys.argv[3]
        class_var = MOM6Historical(
            var=varname,
            data_relative_dir=dict_dir[data_type],
            static_relative_dir='static/',
            grid='raw',
            source='local',
        )
        ds_var = class_var.get_all(freq=freq)

    # for forecast
    if data_type == 'fcst':

        class_var = MOM6Forecast(
            var=varname,
            data_relative_dir=dict_dir[data_type],
            static_relative_dir='static/',
            grid='raw',
            source='local'
        )

        ds_var = class_var.get_all()

    # get source file location
    file_loc = ds_var[varname].encoding['source']
    file_loc_list = file_loc.split('/')
    parent_data_path = f'/{os.path.join(*file_loc_list[:-1])}/'
    filename = file_loc_list[-1]

    if varname == "siconc":
        # cover dim name to the corrected name for Tpoint(error in the model)
        ds_var = ds_var.rename({'xT':'xh','yT':'yh'})

    # change coordinate from -180-180 to 0-360
    try:
        ds_var = ds_var.rename({'geolon':'lon','geolat':'lat'})
    except KeyError as e :
        raise KeyError(
            'Regrid file do not have geolon geolat as coordinate'
        ) from e

    ds_var['lon'] = xr.where(ds_var['lon']<0,ds_var['lon']+360.,ds_var['lon'])

    # Create longitude and latitude arrays (e.g., 1D arrays)
    x = np.linspace(
        ds_var.lon.min(),
        ds_var.lon.max(),
        len(ds_var.xh)-1
    )
    y = np.linspace(
        ds_var.lat.min(),
        ds_var.lat.max(),
        len(ds_var.yh)-1
    )

    # Create a empty dataarray
    data = xr.DataArray(
        data=None,
        coords={'lon': x, 'lat': y},
        dims=('lon', 'lat')
    )

    # Create an xarray dataset with empty dataarray
    ds_regrid = xr.Dataset({'var': data})

    # use xesmf to create regridder
    regridder = xe.Regridder(ds_var, ds_regrid, "bilinear", unmapped_to_nan=True)

    # run regrid
    ds_regrid = xr.Dataset()
    for var in list(ds_var.keys()):
        if var in (varname, f'{varname}_anom'):
            ds_regrid[var] = regridder(ds_var[var]).compute()

    # output the netcdf file
    print(f'output {parent_data_path}regrid/{filename}')
    ds_regrid = MOM6Misc.mom6_encoding_attr(
            ds_var,
            ds_regrid,
            var_names=list(ds_regrid.keys()),
            dataset_name='regional mom6 regrid'
        )
    try:
        ds_regrid.to_netcdf(f'{parent_data_path}regrid/{filename}')
    except PermissionError:
        print(f'{parent_data_path}regrid/{filename} is used by other scripts' )
