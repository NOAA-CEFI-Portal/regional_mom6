"""
This script is designed to do batch calculating the 
climatology (monthly and daily smoothed)

"""
# %%
import os
import glob
import warnings
import xarray as xr
from dask.distributed import Client
from mom6 import DATA_PATH
from mom6.mom6_module.mom6_io import MOM6Misc
from mom6.mom6_module import time_series_processes as tsp

warnings.simplefilter("ignore")

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # %%
    # file location and name
    MOM6_DIR = os.path.join(DATA_PATH,"hist_run/regrid/")
    file_list = glob.glob(f'{MOM6_DIR}/*.nc')
    # %%
    # open each file and calculate climatology
    for file in file_list :
        ds = xr.open_dataset(file)
        ds = ds.isel(time=slice(1,None)) # since the first is 1980
        varname = file.split("/")[-1].split(".")[-2]

        # %%
        if "daily" in file.split("/")[-1].split(".")[0]:
            ds_climo = xr.Dataset()
            da_climo = tsp.cal_daily_climo(
                ds.btm_o2,
                smooth = True,
                dim = 'time',
                nharm = 4,
                apply_taper = False
            )
            ds_climo[varname] = da_climo
        # %%
        else:
            ds_climo = ds.groupby('time.month').mean(dim='time')

        # %%
        # output the netcdf file
        print(f'output {MOM6_DIR}/climo/{file[len(MOM6_DIR):][:-3]}.climo.nc')
        MOM6Misc.mom6_encoding_attr(
                ds,
                ds_climo,
                var_names=[varname],
                dataset_name='regional mom6 regridded climatology'
            )
        ds_climo.to_netcdf(f'{MOM6_DIR}/climo/{file[len(MOM6_DIR):][:-3]}.climo.nc')
