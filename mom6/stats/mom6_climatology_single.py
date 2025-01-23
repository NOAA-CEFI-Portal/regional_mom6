"""
This script is designed to do single variable calculating the 
climatology (monthly and daily smoothed)

"""
import os
import sys
import warnings
import xarray as xr
from dask.distributed import Client
from mom6.mom6_module.deprecated.mom6_io import MOM6Misc, MOM6Historical, MOM6Forecast
from mom6.mom6_module.mom6_statistics import HistoricalClimatology, ForecastClimatology

warnings.simplefilter("ignore")


def get_dir_dict()->dict:
    """different type of data

    Returns
    -------
    dict
        dictionary of different data location
    """
    return {
        'hist_raw': ['hist_run/','static/',{'time':-1,'geolon':50,'geolat':50}],
        'fcst_raw': ['forecast/','static/',{'init':1,'geolon':-1,'geolat':-1}],
        'hist_regrid': ['hist_run/regrid/','',{'time':-1,'lon':50,'lat':50}],
        'fcst_regrid': ['forecast/regrid/','',{'init':1,'lon':-1,'lat':-1}]
    }


# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # check argument exist
    if len(sys.argv) < 2:
        print(
            "Usage: python mom6_climatology_single.py "+
            "<hist/fcst> <raw/regrid> <varnames> <monthly/daily>"
        )
        sys.exit(1)

    data_type = sys.argv[1]
    grid_type = sys.argv[2]
    variable = sys.argv[3]
    freq = sys.argv[4]

    # frequency change to groupby option
    if freq == 'daily':
        GROUPBY_FREQ = 'dayofyear'
    elif freq == 'monthly':
        GROUPBY_FREQ = 'month'
    else:
        raise NameError('the frequency options is only <monthly/daily>')

    # dictionary for input argument to dir name
    dict_dir = get_dir_dict()

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
            chunks=dict_dir[f'{data_type}_{grid_type}'][2]
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

    # create output dataset
    ds_climo = xr.Dataset()
    ds_climo[variable] = da_climo

    file_loc = ds[variable].encoding['source']
    file_loc_list = file_loc.split('/')
    parent_dir = f'/{os.path.join(*file_loc_list[:-1])}/'
    filename = file_loc_list[-1]
    
    ds_climo = MOM6Misc.mom6_encoding_attr(
        ds,
        ds_climo,
        var_names=[variable],
        dataset_name='regional mom6 regridded climatology'
    )    

    if data_type == 'hist':
        # output the netcdf file
        print(f'output {parent_dir}climo/{filename[:-3]}.climo.nc')
        ds_climo.to_netcdf(f'{parent_dir}climo/{filename[:-3]}.climo.nc')
    elif data_type == 'fcst':
        # output the netcdf file
        print(f'output {parent_dir}climo/{filename[:-11]}.climo.nc')
        ds_climo.to_netcdf(f'{parent_dir}climo/{filename[:-11]}.climo.nc')
