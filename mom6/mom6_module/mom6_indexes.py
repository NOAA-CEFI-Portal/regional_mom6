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
