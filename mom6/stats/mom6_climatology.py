"""
This script is designed to do batch calculating the 
climatology (monthly and daily smoothed)

"""
# %%
import warnings
import xarray as xr
from mom6_regrid import mom6_encoding_attr, mom6_hist_run
from dask.distributed import Client
from time_series_processes import cal_daily_climo


warnings.simplefilter("ignore")

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # %%
    # file location and name
    MOM6_DIR = "/Datasets.private/regional_mom6/hist_run/regrid/"
    file_list = mom6_hist_run(MOM6_DIR)

    # %%
    # open each file and calculate climatology
    for file in file_list :
        ds = xr.open_dataset(file)
        ds = ds.isel(time=slice(1,None)) # since the first is 1980
        varname = file.split("/")[-1].split(".")[-2]

        # %%
        if "daily" in file.split("/")[-1].split(".")[0]:
            ds_climo = xr.Dataset()
            da_climo = cal_daily_climo(
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
        mom6_encoding_attr(
                ds,
                ds_climo,
                var_names=varname,
                dataset_name='regional mom6 regridded climatology'
            )
        ds_climo.to_netcdf(f'{MOM6_DIR}/climo/{file[len(MOM6_DIR):][:-3]}.climo.nc')
