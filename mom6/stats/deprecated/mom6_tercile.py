"""
This script is designed to calculate the tercile value based on the
REGRIDDED forecast/forecast data. 

----
future improvement
1. the ability to also deal with the raw grid

"""
# %%
import os
import glob
import sys
import warnings
import xarray as xr
from dask.distributed import Client
from mom6 import DATA_PATH
from mom6.mom6_module.deprecated.mom6_io import MOM6Misc
# from mom6_regrid import mom6_forecast,mom6_encoding_attr
warnings.simplefilter("ignore")

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # check argument exist
    if len(sys.argv) < 3:
        print("Usage: python mom6_tercile.py VARNAME GRIDTYPE")
        sys.exit(1)
    else:
        varname = sys.argv[1]
        grid = sys.argv[2]     # regrid or raw

    # data locations
    if grid == 'raw':
        mom6_dir = os.path.join(DATA_PATH,"forecast/")
        mom6_tercile_dir = os.path.join(DATA_PATH,"tercile_calculation/")
    elif grid == 'regrid':
        mom6_dir = os.path.join(DATA_PATH,"forecast/regrid/")
        mom6_tercile_dir = os.path.join(DATA_PATH,"tercile_calculation/regrid/")
    else:
        print("Usage: python mom6_tercile.py VARNAME GRIDTYPE")
        raise NotImplementedError('GRIDTYPE can only be "raw" or "regrid"')

    file_list = glob.glob(mom6_dir+'/*.nc')
    var_file_list = []
    for file in file_list :
        if varname in file :
            var_file_list.append(file)

    # open regrid file
    for file in var_file_list :
        ds = xr.open_dataset(file)

        # calculate the tercile value
        da_tercile = (
            ds[varname]
            .stack(allens=('init','member'))
            .quantile(
                [1./3.,2./3.],
                dim='allens',
                keep_attrs=True
            )
        )

        ds_tercile = xr.Dataset()
        ds_tercile.attrs = ds.attrs
        ds_tercile['f_lowmid'] = da_tercile.isel(quantile=0)
        ds_tercile['f_midhigh'] = da_tercile.isel(quantile=1)
        ds_tercile = ds_tercile.drop_vars('quantile')

        # output the netcdf file
        print(f'output {mom6_tercile_dir}{file[len(mom6_dir):-6]}tercile_{file[-6:]}')
        MOM6Misc.mom6_encoding_attr(
                ds,
                ds_tercile,
                var_names=list(ds_tercile.keys()),
                dataset_name='regional mom6 regrid tercile'
            )
        try:
            ds_tercile.to_netcdf(f'{mom6_tercile_dir}{file[len(mom6_dir):-6]}tercile_{file[-6:]}',mode='w')
        except PermissionError:
            print(f'{mom6_tercile_dir}{file[len(mom6_dir):-6]}tercile_{file[-6:]} is used by other scripts')
