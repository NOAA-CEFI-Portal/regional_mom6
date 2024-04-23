#!/usr/bin/env python

"""
This is the module to calculate the various indexes from the 
regional MOM6 output shown in Ross et al., 2023

Indexes include:
1. Gulf stream index
"""

import warnings
import numpy as np
import xarray as xr
import xesmf as xe
from mom6.mom6_module import mom6_io

warnings.simplefilter("ignore")
xr.set_options(keep_attrs=True)

class GulfStreamIndex:
    """
    The class is use to recreate the Gulf Stream Index calculation in detail. 
    Original sources are [Ross et al., 2023](https://gmd.copernicus.org/articles/16/6943/2023/).
    and [GFDL CEFI github repository]
    (https://github.com/NOAA-GFDL/CEFI-regional-MOM6/blob/main/diagnostics/physics/ssh_eval.py).
    
    """

    def __init__(
       self,
       ds_data : xr.Dataset,
       ssh_name : str = 'ssh'
    ) -> None:
        """_summary_

        Parameters
        ----------
        ds_data : xr.Dataset
            The sea level height dataset one want to use to 
            derived the gulf stream index. The coordinate
            must have the name "lon" and "lat" exactly
        ssh_name : str
            The sea level height variable name in the dataset 
        """
        self.dataset = ds_data
        self.varname = ssh_name


    @staticmethod
    def __region_focus(
        lon_min : float = -72.,
        lon_max : float = -51.9,
        lat_min : float = 36.,
        lat_max : float = 42.,
        dlon : float = 1.,
        dlat : float = 0.1
    ) -> xr.Dataset:
        """
        Generate the needed Dataset structure for
        regridding purpose.
        
        Currently there is no need for make this 
        method available to user since this should
        be hard coded to maintain the consistence 
        with the Ross et al., 2023. 

        This could be expanded or make available if
        there is need to redefine the region of 
        interest 

        Parameters
        ----------
        lon_min : float, optional
            minimum longitude for the region, by default -72.
        lon_max : float, optional
            maximum longitude for the region, by default -51.9
        lat_min : float, optional
            minimum latitude for the region, by default 36.
        lat_max : float, optional
            maximum longitude for the region, by default 42.
        dlon : float, optional
            resolution of longitude, by default 1.
        dlat : float, optional
            resolution of latitude, by default 0.1

        Returns
        -------
        xr.Dataset
            the regridded Dataset structure
        """


        # longitude coordinate change -180 180 to 0 360
        if lon_max < 0. :
            lon_max += 360.
        if lon_min < 0. :
            lon_min += 360.

        # create the array for regridded lon lat
        x = np.arange(lon_min, lon_max, dlon)
        y = np.arange(lat_min, lat_max, dlat)

        # create an xarray dataset with empty dataarray and designed grid
        data = xr.DataArray(
            data=None,
            coords={'lon': x, 'lat': y},
            dims=('lon', 'lat')
        )
        ds = xr.Dataset({'var': data})

        return ds

    def generate_index(
        self,
    ) -> xr.Dataset:
        """Generate the gulf stream index

        Returns
        -------
        xr.Dataset
            dataset containing the gulf_stream_index
            variables.
        """

        # getting the dataset
        ds_data = self.dataset

        # change longitude range from -180 180 to 0 360
        try:
            lon = ds_data['lon'].data
        except KeyError as e:
            raise KeyError("Coordinates should have 'lon' and 'lat' with exact naming") from e
        lon_ind = np.where(lon<0)
        lon[lon_ind] += 360.
        ds_data['lon'].data = lon
        # ds_data = ds_data.sortby('lon')

        # Define Regridding data structure
        ds_regrid = self.__region_focus()

        # use xesmf to create regridder
        regridder = xe.Regridder(ds_data, ds_regrid, "bilinear", unmapped_to_nan=True)

        # perform regrid for each field
        ds_regrid = xr.Dataset()
        ds_regrid['ssh'] = regridder(ds_data[self.varname])

        # Calculate the Sea Surface Height (SSH) anomaly
        # We calculate the anomaly based on the monthly climatology.
        da_regrid_anom = (
            ds_regrid['ssh'].groupby('time.month')-
            ds_regrid['ssh'].groupby('time.month').mean('time')
        )

        # Calculate the Standard Deviation of SSH Anomaly
        da_std = da_regrid_anom.std('time')

        # Calculate the Latitude of Maximum Standard Deviation
        # - determine the maximum latitude index
        da_lat_ind_maxstd = da_std.argmax('lat').compute()
        da_lat_ind_maxstd.name = 'lat_ind_of_maxstd'

        # - use the maximum latitude index to find the latitude
        da_lat_maxstd = da_std.lat.isel(lat=da_lat_ind_maxstd).compute()
        da_lat_maxstd.name = 'lat_of_maxstd'

        # Calculate the Gulf Stream Index
        # - use the maximum latitude index to find the SSH anomaly along the line shown above.
        # - calculate the longitude mean of the SSH anomaly (time dependent)
        #     $$\text{{SSHa}}$$
        # - calculate the stardarde deviation of the $\text{{SSHa}}$
        #     $$\text{{SSHa\_std}}$$
        # - calculate the index
        #     $$\text{{Gulf Stream Index}} = \frac{\text{{SSHa}}}{\text{{SSHa\_std}}}$$
        da_ssh_mean_along_gs = (
            da_regrid_anom
            .isel(lat=da_lat_ind_maxstd)
            .mean('lon')
        )
        da_ssh_mean_std_along_gs = (
            da_regrid_anom
            .isel(lat=da_lat_ind_maxstd)
            .mean('lon')
            .std('time')
        )
        da_gs_index = da_ssh_mean_along_gs/da_ssh_mean_std_along_gs
        ds_gs = xr.Dataset()
        ds_gs['gulf_stream_index'] = da_gs_index

        return ds_gs

class ColdPoolIndex:
    """
    This class is used to create the Cold Pool Index calculation
    Original sources are [Ross et al., 2023](https://gmd.copernicus.org/articles/16/6943/2023/).
    and [GFDL CEFI github repository]
    (https://github.com/NOAA-GFDL/CEFI-regional-MOM6/blob/main/diagnostics/physics/NWA12/coldpool.py)
    """
    def __init__(
        self,
        ds_data: xr.Dataset,
        bottomT_name: str = 'bottomT'
    ):
        """_summary_
        
        Parameters
        ----------
        ds_data: xr.Dataset
            The bottom temperature dataset used to
            derive the cold pool index.
        bottomT_name" str
            The bottom temperature variable name in the data set
        """
        self.dataset = ds_data
        self.varname = bottomT_name
    
    def mask_region(ds):
        """
        Generate the region mask and regrid data using MOM6 model
        
        Parameters
        ----------
        ds : xr.Dataset
            Dataset that needs to be regridded
        
        Returns
        -------
        da_mask_regrid_crop : xr.Dataset
            Cropped and regridded dataset"""
        
        #Create region mask from MOM6 model
        ds_mab = mom6_io.MOM6Static.get_regionl_mask('masks/')
        ds_grid = mom6_io.MOM6Static.get_grid('')
        ds_mask = xr.merge([ds_mab,ds_grid])
        ds_mask = ds_mask.set_coords(['geolon','geolat','geolon_c','geolat_c','geolon_u','geolat_u','geolon_v','geolat_v'])

        # Regrid the mask to GLORYS
        # Use xesmf to create regridder using bilinear method 
        # !!!! Regridded only suited for geolon and geolat to x and y
        regridder = xe.Regridder(ds_mask.rename({'geolon':'lon','geolat':'lat'}), ds, "bilinear", unmapped_to_nan=True)
        da_mask = xr.where(ds_mask.MAB,x=1,y=np.nan)

        # Perform regrid using adaptive masking
        #  https://pangeo-xesmf.readthedocs.io/en/latest/notebooks/Masking.html#Adaptive-masking
        da_mask_regrid = regridder(da_mask, skipna=True, na_thres=0.25).compute()

        #Crop the region to the southern flank of the Georges Bank
        da_mask_regrid_crop = (da_mask_regrid
            .where(
                (da_mask_regrid.latitude>=38)&
                (da_mask_regrid.latitude<=41.5)&
                (da_mask_regrid.longitude<=-68.5+360)&
                (da_mask_regrid.longitude>=-75+360),
                drop=True
            )
            .where(
                (da_mask_regrid.latitude<=41)&
                (da_mask_regrid.longitude>=-70+360),
                drop=True
            )
        )
        return da_mask_regrid_crop
    
    def index_coldpool(self):
        '''
        Define Coldpool Domain and Calculate Index
        Depth: Between 20m and 200m isobath
        Time: Between June and September from 1959 to 2022
        Temperature: Average bottom temperature was cooler than 10 degrees Celsius (and > 6?)
        Location: Mid-Atlantic Bight (MAB) domain between 38N-41.5N and between 75W-68.5W
        
        Returns
        -------
        da_cpi_mon : xr.Dataset
            Cold pool index calculation based on monthly climatology
        da_cpi_ann : xr.Dataset
            Cold pool index calculation based on yearly climatology'''
        #Get data and regrid it
        ds = self.dataset
        ds_regrid = self.mask_region(ds)
        #Set depth mask per coldpool domain definition
        da_mask_depth = ds.deptho.where((ds.deptho>20.)&(ds.deptho<200.),other=np.nan)
        da_mask_depth = xr.where(da_mask_depth.notnull(),x=1,y=np.nan)

        #Set time mask
        da_bottomT_Jun2Sep = ds.bottomT.where((ds['time.month']>=6)&(ds['time.month']<=9),drop=True)
        da_bottomT_mon_ltm = da_bottomT_Jun2Sep.groupby(da_bottomT_Jun2Sep['time.month']).mean(dim='time').compute()
        da_bottomT_ann_ltm = da_bottomT_Jun2Sep.groupby(da_bottomT_Jun2Sep['time.year']).mean(dim='time').mean(dim='year').compute()
        # da_mask_bottomT_mon = da_bottomT_Jun2Sep.groupby(da_bottomT_Jun2Sep['time.month']).mean(dim='time').compute()
        # da_mask_bottomT_ann = da_bottomT_Jun2Sep.mean(dim='time').compute()

        #Set temperature mask to less than 10 degrees Celsius
        #TODO: Set to temperature mask to greater than 6 degrees Celsius - pending confirmation
        da_mask_bottomT_mon = xr.where(da_bottomT_mon_ltm<10, x=1, y=np.nan)
        da_mask_bottomT_ann = xr.where(da_bottomT_ann_ltm<10, x=1, y=np.nan)

    #Create final cold pool mask using the cropped location mask, temperature mask, and depth mask
        da_mask_total_mon = ds_regrid*da_mask_bottomT_mon*da_mask_depth
        da_mask_total_ann = ds_regrid*da_mask_bottomT_ann*da_mask_depth

        da_bottomT_mon = da_bottomT_Jun2Sep
        da_bottomT_ann = da_bottomT_Jun2Sep.groupby(da_bottomT_Jun2Sep['time.year']).mean(dim='time')

        #Compute Cold Pool Index using the logic found here: https://noaa-edab.github.io/tech-doc/cold_pool.html
        da_cpi_mon = ((da_bottomT_mon.groupby(da_bottomT_Jun2Sep['time.month'])-da_bottomT_mon_ltm)*da_mask_total_mon).compute()
        da_cpi_ann = ((da_bottomT_ann-da_bottomT_ann_ltm)*da_mask_total_ann).compute()

        return da_cpi_mon, da_cpi_ann