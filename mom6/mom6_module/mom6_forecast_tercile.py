#!/usr/bin/env python

"""
The module include the class to determine
the tercile probability of the forecast
or reforecast

"""
from typing import (
    List,
    Union
)
import warnings
import numpy as np
import pandas as pd
import xarray as xr
from scipy.stats import norm as normal
from mom6.mom6_module.mom6_types import TimeGroupByOptions

warnings.simplefilter("ignore")
xr.set_options(keep_attrs=True)

class Tercile:
    """
    Class for calculating tercile probability
    """
    def __init__(
        self,
        ds_data : xr.Dataset,
        var_name : str,
        initialization_name : str = 'init',
        member_name : str = 'member',
        time_frequency : TimeGroupByOptions = 'month'
    ) -> None:
        """
        Parameters
        ----------
        ds_data : xr.Dataset
            The dataset one want to use to 
            derived the forecast statistics.
        var_name : str
            The variable name in the dataset 
        initialization_name : str, optional
            initialization dimension name, by default 'init'
        member_name : str, optional
            ensemble member dimension name, by default 'member'
        time_frequency : TimeGroupByOptions, optional
            name in time frequency to do the time group, by default 'month'
            'year', 'month', 'dayofyear' are the available options.
        """
        # self.dataset = CoordinateWrangle(ds_data).to_360()
        self.dataset = ds_data
        self.varname = var_name
        self.init = initialization_name
        self.mem = member_name
        self.tfreq = time_frequency

    def generate_tercile(
        self,
        start_year : int = 1993,
        end_year : int = 2020,
    ) -> xr.Dataset:
        """Generate the tercile value based on the 
        full reforecast period (has to be long term)

        Parameters
        ----------
        start_year : int, optional
            start year of when the tercile is determined, by default 1993
        end_year : int, optional
            end year of when the tercile is determined, by default 2020

        Returns
        -------
        xr.Dataset
            the tercile of all avialable intialization in different month
            !!!! if a different initial frequency is generated a refactor is needed!!!
        """
        # getting the dataset
        ds_data = self.dataset

        # crop data
        da_data = ds_data[self.varname].sel(
            {
            self.init : slice(f'{start_year}-01',f'{end_year}-12')
            }
        ).persist()

        # find existing initial month
        init_months = list(set(da_data[f'{self.init}.month'].data))

        list_ds_terciles = [] 
        for init_month in init_months:

            # seperate based on initialization time
            da_init = da_data.where(
                da_data[f'{self.init}.month']==init_month,
                drop=True
            ).compute()

            # calculate the tercile value
            da_tercile = (
                da_init
                .stack(allens=(self.init,self.mem))
                .quantile(
                    [1./3.,2./3.],
                    dim='allens',
                    keep_attrs=True
                )
            )

            # save to individal dataset
            ds_tercile = xr.Dataset()
            ds_tercile['month'] = init_month
            ds_tercile = ds_tercile.set_coords('month')
            ds_tercile['f_lowmid'] = da_tercile.isel(quantile=0)
            ds_tercile['f_midhigh'] = da_tercile.isel(quantile=1)
            ds_tercile = ds_tercile.drop_vars('quantile')
            list_ds_terciles.append(ds_tercile)

        # Concatenate all datasets along the 'month' coordinate
        ds_terciles = xr.concat(list_ds_terciles, dim='month')
        ds_terciles.attrs = ds_data.attrs

        return ds_terciles


    # def calculate_tercile_prob(
    #     self,
    #     iyear : int = 1993,
    #     imonth: int = 3,
    #     lead_bins : List[int] = None,
    #     lead_bin : Union[int, float] = None,
    #     lon : Union[int, float] = None,
    #     lat : Union[int, float] = None
    # ) -> xr.Dataset:
    #     """
    #     use single initialization's normal distribution
    #     and pre-defined tercile value based on the long-term 
    #     statistic tercile value to find the probability of
    #     upper ,normal , and lower tercile
        
    #     It also find the largest probability in upper (positive),
    #     normal (0), lower (negative)

    #     Parameters
    #     ----------
    #     lead_bins : List[int]
    #         The `lead_bin` used to binned the leading month result
    #         ex: one can set `lead_bins = [0, 3, 6, 9, 12]` for four seasonal
    #         mean. Default is no binning, lead_bins = None.
        
    #     Returns
    #     -------
    #     xr.Dataset
    #         two variables are in the dataset. (1) tercile_prob 
    #         (2) tercile_prob_max. 

    #         1 is a 4D matrix with the dimension 
    #         of lon x lat x lead x 3. This are the probability of
    #         upper(lon x lat x lead), normal(lon x lat x lead),
    #         and lower tercile(lon x lat x lead)

    #         2 is the 3D matrix of largest probability in upper (positive),
    #         normal (0), lower (negative) with dimension of (lon x lat x lead)
    #     """

    #     # loaded the mom6 raw field
    #     GetFcast = mom6_io.MOM6Forecast(
    #         var = self.var,
    #         data_relative_dir = self.data_relative_dir,
    #         static_relative_dir = self.static_relative_dir,
    #         tercile_relative_dir = self.tercile_relative_dir,
    #         grid = self.grid,
    #         source = self.source,
    #     )

    #     ds_data = GetFcast.get_single(
    #         iyear = iyear,
    #         imonth = imonth
    #     )

    #     # load variable to memory
    #     da_data = ds_data[self.var]

    #     if lead_bins is None:
    #         # average the forecast over the lead bins
    #         da_binned = da_data.rename({'lead': 'lead_bin'})
    #     else:
    #         # setup lead bins to average during forecast lead time
    #         # (should match lead bins used for the historical data
    #         # that created the *_terciles_*.nc
    #         # [0, 3, 6, 9, 12] produces 3-month averages
    #         lead_bin_label = np.arange(0,len(lead_bins)-1)

    #         # average the forecast over the lead bins
    #         da_binned = (
    #             da_data
    #             .groupby_bins('lead', lead_bins, labels=lead_bin_label, right=True)
    #             .mean('lead')
    #             .rename({'lead_bins': 'lead_bin'})
    #         )

    #     if lead_bin is not None:
    #         da_binned = da_binned.sel(lead_bin=lead_bin,method='nearest')
    #     if lon is not None:
    #         da_binned = da_binned.sel(lon=lon,method='nearest')
    #     if lat is not None:
    #         da_binned = da_binned.sel(lat=lat,method='nearest')

    #     # find a normal distribution for each grid cell and lead bin
    #     # from the ensemble mean and standard deviation
    #     #  this is based on 1 initialization
    #     da_mean = da_binned.mean('member')
    #     da_std = da_binned.std('member')
    #     da_dist = normal(loc=da_mean, scale=da_std)

    #     # load the predetermined reforecast/forecast tercile value
    #     #  this is based on 30 years statistic 1993-2023
    #     ds_tercile = GetFcast.get_tercile('grid')
    #     ds_tercile = ds_tercile.sel(init=imonth,method='nearest')

    #     if lead_bins is None:
    #         ds_tercile_binned = ds_tercile.rename({'lead': 'lead_bin'})
    #     else:
    #         # average the forecast over the lead bins
    #         ds_tercile_binned = (
    #             ds_tercile
    #             .groupby_bins('lead', lead_bins, labels=lead_bin_label, right=True)
    #             .mean('lead')
    #             .rename({'lead_bins': 'lead_bin'})
    #         )

    #     if lead_bin is not None:
    #         ds_tercile_binned = ds_tercile_binned.sel(lead_bin=lead_bin,method='nearest')
    #     if lon is not None:
    #         ds_tercile_binned = ds_tercile_binned.sel(lon=lon,method='nearest')
    #     if lat is not None:
    #         ds_tercile_binned = ds_tercile_binned.sel(lat=lat,method='nearest')

    #     # use single initialization's normal distribution
    #     # and pre-defined tercile value to find the
    #     # probability based on the single initialization
    #     # that correspond to the long-term statistic tercile value

    #     #---probability of lower tercile tail
    #     da_low_tercile_prob = xr.DataArray(
    #         da_dist.cdf(ds_tercile_binned['f_lowmid']),
    #         dims=da_mean.dims,
    #         coords=da_mean.coords
    #     )
    #     #---probability of upper tercile tail
    #     da_up_tercile_prob = 1 - xr.DataArray(
    #         da_dist.cdf(ds_tercile_binned['f_midhigh']),
    #         dims=da_mean.dims,
    #         coords=da_mean.coords
    #     )
    #     #---probability of between lower and upper tercile
    #     da_mid_tercile_prob = 1 - da_up_tercile_prob - da_low_tercile_prob

    #     da_tercile_prob = xr.concat(
    #         [da_low_tercile_prob,da_mid_tercile_prob,da_up_tercile_prob],
    #         pd.Index([-1,0,1],name="tercile")
    #     )

    #     # lower tercile max => negative
    #     # nomral tercile max => 0
    #     # upper tercile max => positive
    #     da_tercile_prob_max = (
    #         da_tercile_prob.idxmax(dim='tercile',fill_value=np.nan)*
    #         da_tercile_prob.max(dim='tercile')
    #     )

    #     # create dataset to store the tercile calculation
    #     ds_tercile_prob=xr.Dataset()
    #     ds_tercile_prob['tercile_prob'] = da_tercile_prob
    #     ds_tercile_prob['tercile_prob_max'] = da_tercile_prob_max

    #     return ds_tercile_prob
