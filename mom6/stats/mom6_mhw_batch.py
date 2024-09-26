"""
This script is designed to do sst threhold for 
marine heatwave application

"""
import os
import warnings
import xarray as xr
from dask.distributed import Client
from mom6 import DATA_PATH
from mom6.mom6_module.mom6_io import MOM6Misc, MOM6Forecast
from mom6.mom6_module.mom6_mhw import MarineHeatwaveForecast

warnings.simplefilter("ignore")

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='200GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    VARIABLE = 'tos'
    DATA_DIR = 'forecast/regrid/'
    STATIC_DIR = ''
    GRID = 'regrid'
    SOURCE = 'local'
    CHUNKS = {'init':1, 'lon':100, 'lat':100}
    THRESHOLD = 90

    # for forecast run
    class_forecast=MOM6Forecast(
        var=VARIABLE,
        data_relative_dir=DATA_DIR,
        static_relative_dir=STATIC_DIR,
        grid=GRID,
        source=SOURCE,
        chunks=CHUNKS
    )
    ds = class_forecast.get_all()

    try :
        ds = ds.rename({'geolon':'lon','geolat':'lat'})
    except ValueError:
        pass

    # calculate batch mhw with stats
    class_mhw = MarineHeatwaveForecast(ds,VARIABLE)
    ds_mhw_batch = class_mhw.generate_forecast_batch(
        climo_start_year=1993,
        climo_end_year=2022,
        anom_start_year=1993,
        anom_end_year=2022,
        quantile_threshold=THRESHOLD
    )

    # find file name and parent dir
    file_loc = ds[VARIABLE].encoding['source']
    file_loc_list = file_loc.split('/')
    parent_dir = f'/{os.path.join(*file_loc_list[:-1])}/'
    filename = file_loc_list[-1]

    # create output dataset
    ds_threshold = xr.Dataset()
    var_name = f'{VARIABLE}_threshold{THRESHOLD:02d}'
    ds_threshold[var_name] = ds_mhw_batch[var_name]
    ds_threshold = MOM6Misc.mom6_encoding_attr(
        ds,
        ds_threshold,
        var_names=[var_name],
        dataset_name=f'regional mom6 regridded MHW threshold {THRESHOLD}'
    )
    ds_threshold.to_netcdf(f'{DATA_PATH}/mhw_calculation/forecast/regrid/{filename[:-11]}.mhw.threshold{THRESHOLD}.nc')
    
    ds_mhw_batch = ds_mhw_batch.drop_vars(var_name).compute()
    variables = list(ds_mhw_batch.data_vars)
    for init_time in ds_mhw_batch.init.data:
        ds_single = ds_mhw_batch.sel(init=init_time)
        iyear = ds_single.init.dt.year
        imonth = ds_single.init.dt.month
        ds_single = MOM6Misc.mom6_encoding_attr(
            ds,
            ds_single,
            var_names=variables,
            dataset_name=f'regional mom6 regridded MHW based on threshold {THRESHOLD}'
        )
        # output the netcdf file
        ds_single.to_netcdf(
            f'{DATA_PATH}/mhw_calculation/forecast/regrid/'+
            f'{filename[:-11]}.mhw.threshold{THRESHOLD}.i{iyear:04d}{imonth:02d}.nc'
        )
        

