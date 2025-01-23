"""
This script is designed to do sst threhold for 
marine heatwave application

"""
import os
import sys
import warnings
import xarray as xr
from dask.distributed import Client
from mom6.mom6_module.deprecated.mom6_io import MOM6Misc, MOM6Historical, MOM6Forecast
from mom6.mom6_module.mom6_statistics import ForecastQuantile,HistoricalClimatology,ForecastClimatology
from mom6_climatology_single import get_dir_dict
from mom6 import DATA_PATH

warnings.simplefilter("ignore")

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='200GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # check argument exist
    if len(sys.argv) < 2:
        print(
            "Usage: python mom6_mhw_threshold_single.py "+
            "<hist/fcst> <raw/regrid> <monthly/daily>"
        )
        sys.exit(1)

    data_type = sys.argv[1]
    grid_type = sys.argv[2]
    freq = sys.argv[3]

    # frequency change to groupby option
    if freq == 'daily':
        GROUPBY_FREQ = 'dayofyear'
    elif freq == 'monthly':
        GROUPBY_FREQ = 'month'
    else:
        raise NameError('the frequency options is only <monthly/daily>')

    # dictionary for input argument to dir name
    dict_dir = get_dir_dict()
    variable = 'tob'
    threshold = 90

     # for historical run
    if data_type == 'hist':
        class_histrun=MOM6Historical(
            var=variable,
            data_relative_dir=dict_dir[f'{data_type}_{grid_type}'][0],
            static_relative_dir=dict_dir[f'{data_type}_{grid_type}'][1],
            grid=grid_type,
            source='local',
            chunks=dict_dir[f'{data_type}_{grid_type}'][2]
        )
        ds = class_histrun.get_all(freq=freq)

        try :
            ds = ds.rename({'geolon':'lon','geolat':'lat'})
        except ValueError:
            pass

        # create climatology class
        time_name = list(dict_dir[f'{data_type}_{grid_type}'][2].keys())[0]
        class_climo = HistoricalClimatology(
            ds_data=ds,
            var_name=variable,
            time_name=time_name,
            time_frequency=GROUPBY_FREQ)

        # calculate climatology fixed climatology for historical run
        da_climo = class_climo.generate_climo(
            climo_start_year=1993,
            climo_end_year=2019,
            dask_option='compute'
        )

    # for forecast run
    elif data_type == 'fcst':
        class_forecast=MOM6Forecast(
            var=variable,
            data_relative_dir=dict_dir[f'{data_type}_{grid_type}'][0],
            static_relative_dir=dict_dir[f'{data_type}_{grid_type}'][1],
            grid=grid_type,
            source='local',
            # chunks=dict_dir[f'{data_type}_{grid_type}'][2]
        )
        ds = class_forecast.get_all()

        try :
            ds = ds.rename({'geolon':'lon','geolat':'lat'})
        except ValueError:
            pass

        # create climatology class
        time_name = list(dict_dir[f'{data_type}_{grid_type}'][2].keys())[0]
        class_climo = ForecastClimatology(
            ds_data=ds,
            var_name=variable,
            time_frequency=GROUPBY_FREQ)

        # calculate climatology fixed climatology for forecast
        da_climo = class_climo.generate_climo(
            climo_start_year=1993,
            climo_end_year=2022,
            dask_option='compute'
        )

        da_anom = ds[variable].groupby(f'{time_name}.{GROUPBY_FREQ}')-da_climo
        ds_anom = xr.Dataset()
        ds_anom['tos'] = da_anom.persist()
        # missing lon coord after subtraction
        ds_anom['lon'] = da_climo.lon
        ds_anom = ds_anom.set_coords('lon')

        class_forecast_quantile = ForecastQuantile(ds_anom,'tos')
        da_threshold = class_forecast_quantile.generate_quantile(1993,2022,threshold,dask_obj=True)


    # create output dataset
    ds_threshold = xr.Dataset()
    var_name = f'{variable}_threshold{threshold:02d}'
    ds_threshold[var_name] = da_threshold
    ds_threshold[var_name].attrs['long_name'] = (
        f'{variable} threshold{threshold:02d})'
    )
    ds_threshold[var_name].attrs['units'] = 'degC'
    
    file_loc = ds[variable].encoding['source']
    file_loc_list = file_loc.split('/')
    parent_dir = f'/{os.path.join(*file_loc_list[:-1])}/'
    filename = file_loc_list[-1]

    ds_threshold = MOM6Misc.mom6_encoding_attr(
        ds,
        ds_threshold,
        var_names=[variable],
        dataset_name=f'regional mom6 regridded MHW threshold {threshold}'
    )

    if data_type == 'hist':
        # output the netcdf file
        print(f'output {DATA_PATH}/mhw_calculation/{filename[:-3]}.{grid_type}.mhw.threshold{threshold}.nc')
        ds_threshold.to_netcdf(f'{DATA_PATH}/mhw_calculation/{filename[:-3]}.{grid_type}.mhw.threshold{threshold}.nc')
    elif data_type == 'fcst':
        # output the netcdf file
        print(f'output {DATA_PATH}/mhw_calculation/{filename[:-11]}.{grid_type}.mhw.threshold{threshold}.nc')
        ds_threshold.to_netcdf(f'{DATA_PATH}/mhw_calculation/{filename[:-11]}.{grid_type}.mhw.threshold{threshold}.nc')

