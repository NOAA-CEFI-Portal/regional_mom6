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
        """
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
    (https://github.com/NOAA-GFDL/CEFI-regional-MOM6/blob/main
     /diagnostics/physics/NWA12/coldpool.py)
    """
    def __init__(
        self,
        ds_data: xr.Dataset,
        ds_cpi_mask: xr.Dataset,
        bottom_temp_name: str = 'bottomT',
        mask_name: str = 'CPI_mask'
    ) -> None:
        """
        Parameters
        ----------
        ds_data: xr.Dataset
            The bottom temperature dataset used to
            derive the cold pool index.
        ds_cpi_mask: xr.Dataset
            The CPI mask.
        bottomT_name" str
            The bottom temperature variable name in the data set
        mask_name" str
            The CPI mask variable name in the `ds_cpi_mask`
        """
        self.dataset = ds_data
        self.mask = ds_cpi_mask
        self.varname = bottom_temp_name
        self.maskname = mask_name

    def regrid_and_mask(self)->xr.DataArray:
        """
        Regrid data from MOM6 model to the mask grid 
        to apply the mask 
        
        Returns
        -------
        da_regrid : xr.DataArray
            regridded and masked dataset
        """
        ds_mask = self.mask
        ds_data = self.dataset

        # Regrid the regional MOM6 data to GLORYS grid
        # Use xesmf to create regridder using bilinear method
        # !!!! Regridded only suited for geolon and geolat to x and y
        regridder = xe.Regridder(
            ds_data.rename({'geolon':'lon','geolat':'lat'}),
            ds_mask,
            "bilinear",
            unmapped_to_nan=True
        )

        # Perform regrid using adaptive masking
        #  https://pangeo-xesmf.readthedocs.io/en/latest/
        #  notebooks/Masking.html#Adaptive-masking
        da_regrid = regridder(ds_data[self.varname], skipna=True, na_thres=0.25).compute()
        da_regrid = da_regrid*ds_mask[self.maskname]

        return da_regrid

    def generate_index(self):
        '''
        Masked data to the Coldpool Domain and Calculate Index
        Coldpool Domain:
            Depth: Between 20m and 200m isobath
            Time: Between June and September from 1959 to 2022
            Temperature: Average bottom temperature was cooler than 10 degrees Celsius (and > 6?)
            Location: Mid-Atlantic Bight (MAB) domain between 38N-41.5N and between 75W-68.5W
        
        Returns
        -------
        da_cpi_ann : xr.DataArray
            Yearly cold pool index calculation based on yearly climatology
        
        '''
        #Get masked data and regrid it
        da_regrid = self.regrid_and_mask()

        #Calculate annual time series and long term mean at each grid
        da_tob_jun2sep = da_regrid.where(
            (da_regrid['time.month']>=6)&
            (da_regrid['time.month']<=9),
            drop=True
        )
        da_tob_ann = (
            da_tob_jun2sep
            .groupby(da_tob_jun2sep['time.year'])
            .mean(dim='time')
        ).compute()
        da_tob_ann_ltm = da_tob_ann.mean('year')

        #Compute Cold Pool Index using the logic found here:
        # https://noaa-edab.github.io/tech-doc/cold_pool.html
        da_tob_ann_anom = da_tob_ann-da_tob_ann_ltm
        da_cpi_ann = da_tob_ann_anom.mean(['latitude', 'longitude'])

        return da_cpi_ann
