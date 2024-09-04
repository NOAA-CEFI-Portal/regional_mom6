#!/usr/bin/env python

"""
The module include multiple regional MOM6 
IO class and methods
"""
from typing import (
    Literal,
    List
)
import os
import glob
import warnings
from datetime import date
import requests
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import cftime
import numpy as np
import xarray as xr
from mom6 import DATA_PATH
from mom6.mom6_module.mom6_types import (
    ModelRegionOptions,GridOptions,DataTypeOptions,DataSourceOptions
)

warnings.simplefilter("ignore")
xr.set_options(keep_attrs=True)



class OpenDapStore:
    """class to handle the OPeNDAP request
    """
    def __init__(
            self,
            region : ModelRegionOptions = 'northwest_atlantic',
            grid : GridOptions = 'raw',
            data_type : DataTypeOptions = 'historical'
    ) -> None:
        """
        input for the class to get the opendap data

        Parameters
        ----------
        grid : Literal[&#39;raw&#39;,&#39;regrid&#39;], optional
            The data extracted should be the regridded result or 
            the original model grid (curvilinear), by default 'raw'
        data_type : Literal[&#39;forecast&#39;,&#39;historical&#39;], optional
            This determine the data type the user want to use 
            to calculate the indexes, by default 'historical'

        """
        self.region = region
        self.grid = grid
        self.data_type = data_type


    def get_catalog(self)-> list:
        """Getting the cataloged files

        Returns
        -------
        list
            a list of url in the form of string that 
            provide the locations of the data when
            accessing using opendap

        Raises
        ------
        FileNotFoundError
            When the files is empty that means the init setting 
            or code must have some incorrect pairing. Debug possibly 
            needed.
        """
        # print(self.data_type)
        if self.data_type == 'historical' :
            datatype = 'hist_run'
        elif self.data_type == 'forecast' :
            datatype = 'forecast'
        # print(datatype)

        if self.grid == 'raw' :
            gridtype = ''
        elif self.grid == 'regrid' :
            gridtype = 'regrid/'

        regiontype = self.region

        catalog_url = (
            'https://psl.noaa.gov/thredds/catalog/'+
            f'Projects/CEFI/regional_mom6/{regiontype}/{datatype}/{gridtype}'
        )
        opendap_url = (
            'https://psl.noaa.gov/thredds/dodsC/'+
            f'Projects/CEFI/regional_mom6/{regiontype}/{datatype}/{gridtype}'
        )

        # Send a GET request to the URL
        html_response = requests.get(catalog_url+'catalog.html', timeout=10)

        # Parse the html response
        soup = BeautifulSoup(html_response.text, 'html.parser')

        # get all code tage in a tag in the "content" div
        div_content = soup.find('div', class_='content')
        a_tags = div_content.find_all('a')
        all_file_list = [a_tag.find_all('code')[0].text for a_tag in a_tags]

        # remove regrid file and directory
        files = []
        for file in all_file_list:
            if 'bilinear' not in file:
                if '.nc' in file:
                    files.append(opendap_url+file)
        if not files :
            raise FileNotFoundError

        return files


class MOM6Forecast:
    """
    Class for getting various mom6 reforecast/forecast
    - get the mom6 files from reforecast/forecast
    - get the mom6 tercile from reforecast/forecast
    - get forecast time stamp
    ...
    """
    def __init__(
        self,
        var : str,
        data_relative_dir : str = None,
        static_relative_dir  : str = None,
        tercile_relative_dir : str = None,
        grid : GridOptions = 'raw',
        source : DataSourceOptions = 'local',
    ) -> None:
        """
        input for the class to get the forecast data

        var : str
            variable name one want to exetract from the data
        data_relative_dir : str
            relative path from DATAPATH setup in config file to 
            the actual forecast/reforecast data, by setting 'forecast/'
            which makes the absolute path to DATAPATH/forecast/
        static_relative_dir : str 
            relative path from DATAPATH setup in config file to 
            the actual static file (grid info) data, by setting 'forecast/'
            which makes the absolute path to DATAPATH/forecast/.
            This is needed when setting grid to `raw`
        tercile_relative_dir : str 
            relative path from DATAPATH setup in config file to 
            the actual tercile related file, by setting 'forecast/'
            which makes the absolute path to DATAPATH/forecast/.
            This is needed when using get_tercile method
        grid : Literal[&#39;raw&#39;,&#39;regrid&#39;], optional
            The data extracted should be the regridded result or 
            the original model grid (curvilinear), by default 'raw'
        source : Literal[&#39;local&#39;,&#39;opendap&#39;], optional
            The source where to import the data, by default 'local'

        """
        self.var = var
        self.grid = grid
        self.source = source
        self.data_relative_dir = data_relative_dir
        self.static_relative_dir = static_relative_dir
        self.tercile_relative_dir = tercile_relative_dir


    def get_all(self) -> xr.Dataset:
        """
        Return the mom6 all rawgrid/regridded reforecast/forecast field
        with the static field combined and setting the
        lon lat related variables to coordinate 

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object is the merged dataset of
            all forecast field include in the `file_list`. The
            Dataset object is lazily-loaded.
        """
        if self.grid == 'raw' :
            if self.source == 'local':
                # getting the forecast/reforecast data
                if self.data_relative_dir is None :
                    raise OSError('for local source please input the path to data file')
                else:
                    mom6_dir = os.path.join(DATA_PATH,self.data_relative_dir)
                file_list = glob.glob(f'{mom6_dir}/*.nc')
                # static field
                if self.static_relative_dir is None :
                    raise OSError('for raw grid please input the path to grid file')
                else:
                    ds_static = MOM6Static.get_grid(self.static_relative_dir)
                # setup chuck
                io_chunk = {}
            elif self.source == 'opendap':
                file_list = OpenDapStore(grid=self.grid,data_type='forecast').get_catalog()
                for file in file_list:
                    var_flag = 'static' in file
                    if var_flag :
                        ds_static = xr.open_dataset(file)
                io_chunk = {'init': 4,'member':1,'lead':-1}

            file_read = [file for file in file_list if f'{self.var}_' in file]

            # merge the static field with the variables
            ds = xr.open_mfdataset(
                file_read,
                combine='nested',
                concat_dim='init',
                chunks=io_chunk
            ).sortby('init')
            ds = xr.merge([ds_static,ds])
            ds = ds.drop_vars(['time'])         # a result of merge of ds_static

            # test if accident read regrid file
            try:
                test_regrid_lon = ds['lon']
                test_regrid_lat = ds['lat']
                raise OSError(
                    'regrid file should not have '+
                    f'lon({len(test_regrid_lon)}) lat({len(test_regrid_lat)}) dim. '+
                    'Check data directory path or grid setting!')
            except KeyError:
                pass

        elif self.grid == 'regrid':
            if self.source == 'local':
                # getting the forecast/reforecast data
                if self.data_relative_dir is None :
                    raise OSError('for local source please input the path to data file')
                else:
                    mom6_dir = os.path.join(DATA_PATH,self.data_relative_dir)
                file_list = glob.glob(f'{mom6_dir}/*.nc')
                io_chunk = {}
            elif self.source == 'opendap':
                file_list = OpenDapStore(grid=self.grid,data_type='forecast').get_catalog()
                io_chunk = {'init': 1,'member':1,'lead':-1}

            file_read = [file for file in file_list if f'{self.var}_' in file]
            ds = xr.open_mfdataset(
                file_read,combine='nested',
                concat_dim='init',
                chunks=io_chunk
            ).sortby('init')

            # test if accident read raw file
            try:
                test_raw_x = ds['xh']
                test_raw_y = ds['yh']
                raise OSError(
                    'regrid file should not have '+
                    f'xh({len(test_raw_x)}) yh({len(test_raw_y)}) dim. '+
                    'Check data directory path or grid setting!')
            except KeyError:
                pass

        return ds

    def get_tercile(
        self,
        average_type : Literal['grid','region'] = 'grid'
    ) -> xr.Dataset:
        """return the mom6 tercile from the forecast

        Parameters
        ----------
        average_type :  Literal[&#39;grid&#39;,&#39;region&#39;], optional
            The type of data. Gridded choose 'grid' and regional 
            area averaged choose 'region', by default 'grid'

        Returns
        -------
        xr.Dataset
            A dataset that include the f_lowmid and f_midhigh value which 
            represent SST values at the boundaries between the terciles. 
            `f_lowmid` represent the boundary value between lower and middle
            tercile. `f_midhigh` represent the boundary value between middle
            and upper tercile. (the filename 'quantile' MIGHT be error naming)
        """
        if self.grid == 'raw' :
            if self.source == 'local':
                # getting the forecast/reforecast data
                if self.tercile_relative_dir is None :
                    raise OSError('for local source please input the path to tercile file')
                else:
                    mom6_dir = os.path.join(DATA_PATH,self.tercile_relative_dir)
                file_list = glob.glob(f'{mom6_dir}/*.nc')
                # static field
                if self.static_relative_dir is None :
                    raise OSError('for raw grid please input the path to grid file')
                else:
                    ds_static = MOM6Static.get_grid(self.static_relative_dir)
                io_chunk = {}
            elif self.source == 'opendap':
                file_list = OpenDapStore(grid=self.grid,data_type='forecast').get_catalog()
                for file in file_list:
                    var_flag = 'static' in file
                    if var_flag :
                        ds_static = xr.open_dataset(file)
                io_chunk = {'init': 4,'member':1,'lead':-1}

            # refine based on var name
            file_read = [file for file in file_list if f'{self.var}_' in file]
            # refine based on region
            if average_type == 'grid':
                file_read = [file for file in file_read if '.region.' not in file]
            elif average_type == 'region':
                file_read = [file for file in file_read if '.region.' in file]

            # merge the static field with the variables
            ds = xr.open_mfdataset(
                file_read,
                combine='nested',
                concat_dim='init',
                chunks=io_chunk
            ).sortby('init')
            ds = xr.merge([ds_static,ds])
            ds = ds.drop_vars(['time'])         # a result of merge of ds_static

            # test if accident read regrid file
            try:
                test_regrid_lon = ds['lon']
                test_regrid_lat = ds['lat']
                raise OSError(
                    'regrid file should not have '+
                    f'lon({len(test_regrid_lon)}) lat({len(test_regrid_lat)}) dim. '+
                    'Check data directory path or grid setting!')
            except KeyError:
                pass

        elif self.grid == 'regrid':
            if self.source == 'local':
                # getting the forecast/reforecast data
                if self.tercile_relative_dir is None :
                    raise OSError('for local source please input the path to data file')
                else:
                    mom6_dir = os.path.join(DATA_PATH,self.tercile_relative_dir)
                file_list = glob.glob(f'{mom6_dir}/*.nc')
                io_chunk = {}
            elif self.source == 'opendap':
                file_list = OpenDapStore(grid=self.grid,data_type='forecast').get_catalog()
                io_chunk = {'init': 4,'member':1,'lead':-1}

            file_read = [file for file in file_list if f'{self.var}_' in file]

            # refine based on region
            if average_type == 'grid':
                file_read = [file for file in file_read if '.region.' not in file]
            elif average_type == 'region':
                file_read = [file for file in file_read if '.region.' in file]

            ds = xr.open_mfdataset(
                file_read,combine='nested',
                concat_dim='init',
                chunks=io_chunk
            ).sortby('init')

            # test if accident read raw file
            try:
                test_raw_x = ds['xh']
                test_raw_y = ds['yh']
                raise OSError(
                    'regrid file should not have '+
                    f'xh({len(test_raw_x)}) yh({len(test_raw_y)}) dim. '+
                    'Check data directory path or grid setting!')
            except KeyError:
                pass

        return ds

    def get_single(
        self,
        iyear : int = 1993,
        imonth : int = 3,
    ) -> xr.Dataset:
        """
        Return the mom6 rawgrid/regridded reforecast/forecast field
        with the static field combined and setting the
        lon lat related variables to coordinate 

        Parameters
        ----------
        iyear : int
            initial year of forecast
        imonth : int
            initial month

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object is the merged dataset of
            all forecast field include in the `file_list`. The
            Dataset object is lazily-loaded.
        """
        ds = self.get_all()

        min_year = ds['init.year'].min().data
        max_year = ds['init.year'].max().data

        ds = ds.sel(init=f'{iyear}-{imonth:02d}',method='nearest')

        if ds['init.year'].data != iyear or ds['init.month'].data != imonth:
            raise IndexError(
                'input iyear and imonth out of data range, '+
                f'{min_year} <= iyear <= {max_year}, '+
                'imonth can only be 3,6,9,12'
            )

        return ds


    def get_init_fcst_time(
        self,
        iyear : int = 1993,
        imonth : int = 3,
        lead_bins : List[int] = None
    ) -> dict:
        """Setup the initial and forecast time format for output

        Parameters
        ----------
        iyear : int
            initial year of forecast
        imonth : int
            initial month
        lead_bins : List[int]
            The `lead_bin` used to binned the leading month result
            example is `lead_bins = [0, 3, 6, 9, 12]` for four seasonal
            mean.

        Returns
        -------
        dict
            with two key-value pairs, 'init': initial_time and
            'fcst': mean forecasted during the binned period 
        """
        if lead_bins is None:
            lead_bins = [0, 3, 6, 9, 12]

        # get the cftime of initial time
        btime = cftime.datetime(iyear,imonth,1)

        # store the forecast time format based on all leadtime
        forecasttime = []
        period_length = lead_bins[1]-lead_bins[0]-1  # assuming the bins are equal space
        for l in range(0,12):
            # leadtime period start
            sdate = (
                date.fromisoformat(f'{btime.year}-'+
                                    f'{btime.month:02d}-'+
                                    f'{1:02d}')
                +relativedelta(months=l)
            )
            # leadtime period end
            fdate = (
                date.fromisoformat(f'{btime.year}-'+
                                    f'{btime.month:02d}-'+
                                    f'{1:02d}')
                +relativedelta(months=l+period_length)
            )
            # store array of forecast 3 month period
            forecasttime.append(f'{sdate.strftime("%b")}-{fdate.strftime("%b %Y")}')

        # construct forecast period only during the binned period
        mean_forecasttime = [forecasttime[idx] for idx in lead_bins[:-1]]

        # get the initial time
        ini_time_date = (
            date.fromisoformat(
                f'{btime.year}-'+
                f'{btime.month:02d}-'+
                f'{1:02d}'
            )
        )
        # construct the initial time format
        ini_time = f'{ini_time_date.strftime("%b %Y")}'

        return {'init':ini_time,'fcsts':mean_forecasttime}


class MOM6Historical:
    """
    Class for various mom6 historical run IO

    """
    def __init__(
        self,
        var : str,
        data_relative_dir : str = None,
        static_relative_dir  : str = None,
        grid : GridOptions = 'raw',
        source : DataSourceOptions = 'local',
    ) -> None:
        """
        input for getting the historical run data

        Parameters
        ----------
        var : str
            variable name one want to exetract from the data
        data_relative_dir : str
            relative path from DATAPATH setup in config file to 
            the actual historical run data, by setting 'hist_run/'
            which makes the absolute path to DATAPATH/hist_run/
        static_relative_dir : str 
            relative path from DATAPATH setup in config file to 
            the actual static file (grid info) data, by setting 'static/'
            which makes the absolute path to DATAPATH/static/.
            This is needed when setting grid to `raw`
        grid : Literal[&#39;raw&#39;,&#39;regrid&#39;], optional
            The data extracted should be the regridded result or 
            the original model grid (curvilinear), by default 'raw'
        source : Literal[&#39;local&#39;,&#39;opendap&#39;], optional
            The source where to import the data, by default 'local'

        """
        self.var = var
        self.grid = grid
        self.source = source
        self.data_relative_dir = data_relative_dir
        self.static_relative_dir = static_relative_dir

    def get_all(self) -> xr.Dataset:
        """
        Return the mom6 all rawgrid/regridded historical run field
        with the static field combined and setting the
        lon lat related variables to coordinate 

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object is the merged dataset of
            all forecast field include in the `file_list`. The
            Dataset object is lazily-loaded.
        """
        if self.grid == 'raw' :
            if self.source == 'local':
                # getting the historical run data
                if self.data_relative_dir is None :
                    raise OSError('for local source please input the path to data file')
                else:
                    mom6_dir = os.path.join(DATA_PATH,self.data_relative_dir)
                file_list = glob.glob(f'{mom6_dir}/*.nc')
                # static field
                if self.static_relative_dir is None :
                    raise IOError('for raw grid please input the path to grid file')
                else:
                    ds_static = MOM6Static.get_grid(self.static_relative_dir)
                io_chunk = {}
            elif self.source == 'opendap':
                file_list = OpenDapStore(grid=self.grid,data_type='historical').get_catalog()
                for file in file_list:
                    var_flag = 'static' in file
                    if var_flag :
                        ds_static = xr.open_dataset(file)
                io_chunk = {'time': 100}

            file_read = [file for file in file_list if f'.{self.var}.' in file]

            # merge the static field with the variables
            ds = xr.open_mfdataset(
                file_read,combine='nested',
                concat_dim='time',
                chunks=io_chunk
            ).sortby('time')
            ds = xr.merge([ds_static,ds])
            ds = ds.isel(time=slice(1,None))  # exclude the 1980 empty field due to merge

            # test if accident read regrid file
            try:
                test_regrid_lon = ds['lon']
                test_regrid_lat = ds['lat']
                raise OSError(
                    'regrid file should not have '+
                    f'lon({len(test_regrid_lon)}) lat({len(test_regrid_lat)}) dim. '+
                    'Check data directory path or grid setting!')
            except KeyError:
                pass

        elif self.grid == 'regrid':
            if self.source == 'local':
                # getting the historical run data
                if self.data_relative_dir is None :
                    raise OSError('for local source please input the path to data file')
                else:
                    mom6_dir = os.path.join(DATA_PATH,self.data_relative_dir)
                file_list = glob.glob(f'{mom6_dir}/*.nc')
            elif self.source == 'opendap':
                file_list = OpenDapStore(grid=self.grid,data_type='historical').get_catalog()

            file_read = [file for file in file_list if f'.{self.var}.' in file]
            ds = xr.open_mfdataset(
                file_read,
                combine='nested',
                concat_dim='time',
                chunks={'time': 100}
            ).sortby('time')

            # test if accident read raw file
            try:
                test_raw_x = ds['xh']
                test_raw_y = ds['yh']
                raise OSError(
                    'regrid file should not have '+
                    f'xh({len(test_raw_x)}) yh({len(test_raw_y)}) dim. '+
                    'Check data directory path or grid setting!')
            except KeyError:
                pass

        return ds

    def get_single(
        self,
        year : int = 1993,
        month : int = 1,
        day : int = 1,
    ) -> xr.Dataset:
        """
        Return the mom6 rawgrid/regridded historical run field
        with the static field combined and setting the
        lon lat related variables to coordinate 

        Parameters
        -------
        year : int
            year of historical run
        month : int
            month of the historical run
        day : int
            day in month of the historical run

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object is the merged dataset of
            all forecast field include in the `file_list`. The
            Dataset object is lazily-loaded.
        """
        ds = self.get_all()

        min_year = ds['time.year'].min().data
        max_year = ds['time.year'].max().data
        delta_t = (
            ds.isel(time=1).time.data-ds.isel(time=0).time.data
        ).astype('timedelta64[D]')/np.timedelta64(1,'D')
        days = int(delta_t)
        if days > 1:
            ds = ds.sel(time=f'{year}-{month:02d}-15',method='nearest')

            if (ds['time.year'].data != year or
                ds['time.month'].data != month):
                raise IndexError(
                    'input year and month out of data range, '+
                    f'{min_year} <= year <= {max_year}, '+
                    '1 <= month <= 12'
                )
        if days <= 1:
            ds = ds.sel(time=f'{year}-{month:02d}-{day:02d}',method='nearest')

            if (ds['time.year'].data != year or
                ds['time.month'].data != month or
                ds['time.day'].data != day):
                raise IndexError(
                    'input year and month out of data range, '+
                    f'{min_year} <= year <= {max_year}, '+
                    '1 <= month <= 12'
                    '1 <= day <= 31'
                )

        return ds

class MOM6Static:
    """
    Class for getting various Static field
    1. regional mask in raw regional mom6 grid
    2. static file for grid and mask information
    ...
    """
    @staticmethod
    def get_regionl_mask(
        data_relative_dir : str
    ) -> xr.Dataset:
        """return the EPU mask in the original mom6 grid
        Parameters
        ----------
        data_relative_dir : str
            relative path from DATAPATH setup in config file to 
            the actual forecast/reforecast data, by setting 'forecast/'
            which makes the absolute path to DATAPATH/forecast/

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object of regional EPU mask
        """
        ds = xr.open_dataset(os.path.join(DATA_PATH,data_relative_dir,"region_masks.nc"))
        ds = ds.set_coords(['geolon','geolat'])
        # change the boolean to 1,nan for mask
        for var in list(ds.keys()):
            if var not in ['areacello', 'geolat', 'geolon']:
                ds[var] = xr.where(ds[var],1.,np.nan)

        return ds

    @staticmethod
    def get_cpi_mask(
        data_relative_dir : str
    ) -> xr.Dataset:
        """return the Cold Pool Index mask in the GLORYS grid.
        
        The mask is currently derived by Chia-Wei Hsu based 
        solely on the avialable GLORYS data. 

        The mask has three main criterias
        1. within EPU MAB (Mid-Atlantic Bight)
           => within (38N-41.5N,75W-68.5W) 
           => within (<41N, <70W)
        2. Only consider bottom temperature between 20m-200m isobath 
        3. Long term mean (1993-2022) of annual mean (Jun-Sep) cooler than 10degC

        Parameters
        ----------
        data_relative_dir : str
            relative path from DATAPATH setup in config file to 
            the actual mask data, by setting 'masks/'
            which makes the absolute path to DATAPATH/masks/

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object of CPI mask in GLORYS grid
        """
        return xr.open_dataset(os.path.join(DATA_PATH,data_relative_dir,"cpi_mask.nc"))

    @staticmethod
    def get_grid(
        data_relative_dir : str
    ) -> xr.Dataset:
        """return the original mom6 grid information

        Parameters
        ----------
        data_relative_dir : str
            relative path from DATAPATH setup in config file to 
            the actual forecast/reforecast data, by setting 'forecast/'
            which makes the absolute path to DATAPATH/forecast/

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object of mom6's grid lon lat
        """
        ds_static = xr.open_dataset(os.path.join(DATA_PATH,data_relative_dir,'ocean_static.nc'))
        return ds_static.set_coords(
            ['geolon','geolat',
            'geolon_c','geolat_c',
            'geolon_u','geolat_u',
            'geolon_v','geolat_v']
        )

    @staticmethod
    def get_rotate(
        data_relative_dir : str
    ) -> xr.Dataset:
        """return the original mom6 grid rotation information

        The information is store in the ice_month.static.nc file

        Parameters
        ----------
        data_relative_dir : str
            relative path from DATAPATH setup in config file to 
            the actual forecast/reforecast data, by setting 'forecast/'
            which makes the absolute path to DATAPATH/forecast/

        Returns
        -------
        xr.Dataset
            The Xarray Dataset object of mom6's grid lon lat
        """
        ds_rotate = xr.open_dataset(
            os.path.join(DATA_PATH,data_relative_dir,'ice_monthly.static.nc')
        )

        # prepare the rotation matrix to regular coord names
        ds_rotate = ds_rotate.rename({
            'yT':'yh',
            'xT':'xh',
            'GEOLON':'geolon',
            'GEOLAT':'geolat',
            'COSROT':'cosrot',
            'SINROT':'sinrot'
        })

        return ds_rotate.set_coords(
            ['geolon','geolat']
        )

    @staticmethod
    def get_mask(
        data_relative_dir : str,
        mask : Literal['wet','wet_c','wet_u','wet_v'] = 'wet',
    ) -> xr.DataArray:
        """
        The function is designed to export the various mask provided
        on the MOM6 grid from the ocean_static.nc file

        Parameters
        ----------
        data_relative_dir : str
            relative path from DATAPATH setup in config file to 
            the actual forecast/reforecast data, by setting 'forecast/'
            which makes the absolute path to DATAPATH/forecast/

        mask : str
            The mask name based on the variable name in the ocean_static.nc file.
            It has the following options 1. wet (0 if land, 1 if ocean at
            tracer points), 2. wet_c (0 if land, 1 if ocean at corner (Bu)
            points), 3. wet_u (0 if land, 1 if ocean at zonal velocity (Cu) 
            points), 4. wet_v (0 if land, 1 if ocean at meridional velocity
            (Cv) points), by default 'wet'.

        Returns
        -------
        xr.DataArray
            The Xarray DataArray object that represent the ocean mask.
        """

        ds = xr.open_dataset(os.path.join(DATA_PATH,data_relative_dir,'ocean_static.nc'))
        try:
            da = ds.set_coords(['geolon','geolat'])[mask]
        except KeyError :
            pass
        return da


class MOM6Misc:
    """MOM6 related methods 
    """
    @staticmethod
    def mom6_historical(
        historical_dir : str
    ) -> List[str]:
        """
        Create list of files to be able to be opened 
        by Xarray.

        Parameters
        ----------
        historical_dir : str
            directory path in string to the historical run

        Returns
        -------
        List 
            A list of all data name including directory path 
            for the historical run data
        """

        # h point list
        hpoint_file_list = [
            "ocean_monthly.199301-201912.MLD_003.nc",
            "ocean_monthly.199301-201912.sos.nc",
            "ocean_monthly.199301-201912.ssh.nc",
            "ocean_monthly.199301-201912.tob.nc",
            "ocean_monthly.199301-201912.tos.nc",
            "ocean_monthly_z.199301-201912.so.nc",
            "ocean_monthly_z.199301-201912.thetao.nc",
            "ocean_cobalt_daily_2d.19930101-20191231.btm_o2.nc",
            "ocean_cobalt_omip_sfc.199301-201912.chlos.nc",
            "ocean_cobalt_omip_sfc.199301-201912.dissicos.nc",
            "ocean_cobalt_omip_sfc.199301-201912.talkos.nc",
            "ocean_cobalt_sfc.199301-201912.sfc_co3_ion.nc",
            "ocean_cobalt_sfc.199301-201912.sfc_co3_sol_arag.nc",
            "ocean_cobalt_sfc.199301-201912.sfc_no3.nc",
            "ocean_cobalt_sfc.199301-201912.sfc_po4.nc",
            "ocean_cobalt_tracers_int.199301-201912.mesozoo_200.nc"
        ]
        hpoint_file_list = [f"{historical_dir}{file}" for file in hpoint_file_list]

        # T point that is the same as h point
        tpoint_file_list = [
            "ice_monthly.199301-201912.siconc.nc"
        ]
        tpoint_file_list = [f"{historical_dir}{file}" for file in tpoint_file_list]

        all_file_list = hpoint_file_list+tpoint_file_list

        return all_file_list

    @staticmethod
    def mom6_forecast(
        forecast_dir : str
    ) -> List[str]:
        """
        Create list of files to be able to be opened 
        by Xarray.

        Parameters
        ----------
        forecast_dir : str
            directory path in string to the forecast/reforecast

        Returns
        -------
        List 
            A list of all data name including directory path 
            for the reforecast/forecast data
           
        """
        # input of array of different variable forecast
        tob_files = []
        tos_files = []
        for year in range(1993,2022+1):
            for month in range(3,13,3):
                tob_files.append(f"tob_forecasts_i{year:04d}{month:02d}.nc")
                tob_files.append(f"tos_forecasts_i{year:04d}{month:02d}.nc")

        # h point list
        hpoint_file_list = (
            tob_files+
            tos_files
        )

        hpoint_file_list = [f"{forecast_dir}{file}" for file in hpoint_file_list]

        all_file_list = hpoint_file_list


        return all_file_list

    @staticmethod
    def mom6_encoding_attr(
        ds_data_ori : xr.Dataset,
        ds_data : xr.Dataset,
        dataset_name : str,
        var_names : List[str] = None
    ):
        """
        This function is designed for creating attribute and netCDF encoding
        for the preprocessed regional mom6 file format.

        Parameters
        ----------
        ds_data_ori : xr.Dataset
            original dataset
        ds_data : xr.Dataset
            new output regridded dataset
        var_name : string
            var name in the dataset
        dataset : string
            name of the dataset 

        Returns
        -------
        ds_data : xr.Dataset
            new output regridded dataset with attr and encoding setup.
        
        Raises
        ------

        """

        # besides lon lat which has PSL format
        #  put all dim name that may encounter in
        #  different mom6 output. These dims will
        #  follow its original data attr and encoding
        misc_dims_list = ['time','lead','init','member','z_l']

        if var_names is None:
            var_names = []

        # global attrs and encoding
        ds_data.attrs = ds_data_ori.attrs
        ds_data.encoding = ds_data_ori.encoding
        ds_data.attrs['history'] = "Derived and written at NOAA Physical Science Laboratory"
        ds_data.attrs['contact'] = "chia-wei.hsu@noaa.gov"
        ds_data.attrs['dataset'] = dataset_name

        try:
            # lon and lat attrs and encoding (PSL format)
            # longitude attrs
            ds_data['lon'].attrs = {
                'standard_name' : 'longitude',
                'long_name' : 'longitude',
                'units' : 'degrees_east',
                'axis' : 'X',
                'actual_range' : (
                    np.float64(ds_data['lon'].min()),
                    np.float64(ds_data['lon'].max())
                )
            }
            ds_data['lon'].encoding = {
                'zlib': True,
                'szip': False,
                'zstd': False,
                'bzip2': False,
                'blosc': False,
                'shuffle': True,
                'complevel': 2,
                'fletcher32': False,
                'contiguous': False,
                'chunksizes': [len(ds_data['lon'].data)],
                'original_shape': [len(ds_data['lon'].data)],
                'dtype': 'float64'}
        except KeyError:
            print('no lon dimension')

        try:
            # latitude attrs
            ds_data['lat'].attrs = {
                'standard_name' : 'latitude',
                'long_name' : 'latitude',
                'units' : 'degrees_north',
                'axis' : 'Y',
                'actual_range' : (
                    np.float64(ds_data['lat'].min()),
                    np.float64(ds_data['lat'].max())
                )
            }
            ds_data['lat'].encoding = {
                'zlib': True,
                'szip': False,
                'zstd': False,
                'bzip2': False,
                'blosc': False,
                'shuffle': True,
                'complevel': 2,
                'fletcher32': False,
                'contiguous': False,
                'chunksizes': [len(ds_data['lon'].data)],
                'original_shape': [len(ds_data['lon'].data)],
                'dtype': 'float64'}
        except KeyError:
            print('no lat dimension')

        # copy original attrs and encoding for dims
        for dim in misc_dims_list:
            try:
                ds_data[dim].attrs = ds_data_ori[dim].attrs
                ds_data[dim].encoding = ds_data_ori[dim].encoding
                ds_data[dim].encoding['complevel'] = 2
            except KeyError:
                print(f'no {dim} dimension')

        # copy original attrs and encoding for variables
        for var_name in var_names:
            try:
                ds_data[var_name].attrs = ds_data_ori[var_name].attrs
                ds_data[var_name].encoding = ds_data_ori[var_name].encoding
            except KeyError:
                print(f'new variable name {var_name}')
                ds_data[var_name].encoding['complevel'] = 2

        return ds_data
