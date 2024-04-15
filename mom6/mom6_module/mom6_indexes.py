#!/usr/bin/env python

"""
This is the module to calculate the various indexes from the 
regional MOM6 output shown in Ross et al., 2023

Indexes include:
1. Gulf stream index
"""
from typing import (
    Literal,
    List,
    Union
)
import os
import warnings
import numpy as np
import xarray as xr
import xesmf as xe
from mom6 import DATA_PATH
from mom6.mom6_module import mom6_process as mp


warnings.simplefilter("ignore")
xr.set_options(keep_attrs=True)

class GulfStreamIndex:
    """
    The class is use to recreate the Gulf Stream Index calculation in detail. 
    Original sources are [Ross et al., 2023](https://gmd.copernicus.org/articles/16/6943/2023/).
    and [GFDL CEFI github repository](https://github.com/NOAA-GFDL/CEFI-regional-MOM6/blob/main/diagnostics/physics/ssh_eval.py).
    
    """

    def __init__(
       self,
       data_type : Literal['forecast','historical'] = 'forecast',
       grid : Literal['raw','regrid'] = 'regrid'
    ) -> None:
        """
        input for the class to determine what data the user 
        want the index to be calculated from.

        Parameters
        ----------
        data_type : Literal[&#39;forecast&#39;,&#39;historical&#39;], optional
            This determine the data type the user want to use 
            to calculate the indexes, by default 'forecast'
        grid : Literal[&#39;raw&#39;,&#39;regrid&#39;], optional
            This determine the type of grid solution the user 
            want to use to calculate the indexes, by default 'regrid'
        """
        self.data_type = data_type
        self.grid = grid

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
        
        # getting the dataset
        if self.data_type in ['historical']:
            ds_data = mp.MOM6Historical.get_mom6_all('ssh',grid=self.grid)
        elif self.data_type in ['forecast']:
            ds_data = mp.MOM6Forecast.get_mom6_all('ssh',grid=self.grid)        

        # change longitude range from -180 180 to 0 360
        ds_data['geolon'] = ds_data['geolon']+360.

        # Define Regridding data structure
        ds_regrid = self.__region_focus()

        # use xesmf to create regridder
        if self.grid in ['raw']:
            ds_data = ds_data.rename({'geolon':'lon','geolat':'lat'})
        regridder = xe.Regridder(ds_data, ds_regrid, "bilinear", unmapped_to_nan=True)

        # perform regrid for each field
        ds_regrid = xr.Dataset()
        ds_regrid['ssh'] = regridder(ds_data['ssh'])

# %% [markdown]
# ## Calculate the Sea Surface Height (SSH) anomaly
# We calculate the anomaly based on the monthly climatology.

# %%
da_regrid_anom = ds_regrid['ssh'].groupby('time.month')-ds_regrid['ssh'].groupby('time.month').mean('time')

# %%
da_regrid_anom.isel(time=1).plot()

# %% [markdown]
# ## Calculate the Standard Deviation of SSH Anomaly

# %%
da_std = da_regrid_anom.std('time')
da_std.plot()

# %% [markdown]
# ## Calculate the Latitude of Maximum Standard Deviation
# - determine the maximum latitude index
# - use the maximum latitude index to find the latitude

# %%
da_lat_ind_maxstd = da_std.argmax('lat').compute()
da_lat_ind_maxstd.name = 'lat_ind_of_maxstd'
da_lat_ind_maxstd.plot()

# %%
da_lat_maxstd = da_std.lat.isel(lat=da_lat_ind_maxstd).compute()
da_lat_maxstd.name = 'lat_of_maxstd'
da_lat_maxstd.plot()

# %% [markdown]
# ## Calculate the Gulf Stream Index
# - use the maximum latitude index to find the SSH anomaly along the line shown above.
# - calculate the longitude mean of the SSH anomaly (time dependent) 
#     $$\text{{SSHa}}$$
# - calculate the stardarde deviation of the $\text{{SSHa}}$
#     $$\text{{SSHa\_std}}$$
# - calculate the index
#     $$\text{{Gulf Stream Index}} = \frac{\text{{SSHa}}}{\text{{SSHa\_std}}}$$

# %%
da_ssh_mean_along_gs = da_regrid_anom.isel(lat=da_lat_ind_maxstd).mean('lon')
da_ssh_mean_std_along_gs = da_regrid_anom.isel(lat=da_lat_ind_maxstd).mean('lon').std('time')
da_gs_index = da_ssh_mean_along_gs/da_ssh_mean_std_along_gs

# %%
da_gs_index.plot()


