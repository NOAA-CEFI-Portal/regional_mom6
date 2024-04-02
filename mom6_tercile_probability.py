#!/usr/bin/env python

"""
This is the script to genearte the tercile plot based on the 
forecast(hindcast) that is generated from Andrew Ross at GFDL.

There are two types of tercile plots generated
1. Ecological Production Units (EPU based) tercile plot
2. Grid-point wise tercile plot

"""
from typing import (
    Literal,
    List
)
from datetime import date
from dateutil.relativedelta import relativedelta
import cftime
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.colors import ListedColormap
from matplotlib import cm
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from scipy.stats import norm as normal
from mom6_regrid import mom6_hindcast




def get_mom6_regionl_mask() -> xr.Dataset:
    """return the EPU mask in the mom6 grid
    """
    ds = xr.open_dataset('/Datasets.private/regional_mom6/masks/region_masks.nc')
    return ds.set_coords(['geolon','geolat'])

def get_mom6_mask(
    mask:Literal['wet','wet_c','wet_u','wet_v'] = 'wet'
) -> xr.DataArray:
    """
    The function is designed to export the various mask provided
    on the MOM6 grid from the ocean_static.nc file

    Parameters
    ----------
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

    ds = xr.open_dataset('/Datasets.private/regional_mom6/ocean_static.nc')
    return ds.set_coords(['geolon','geolat'])[mask]


def get_mom6_raw(
    imonth : int,
    var : str,
) -> xr.Dataset:
    """
    Return the mom6 raw grid hindcast/forecast field
    with the static field combined and setting the
    lon lat related variables to coordinate 

    Returns
    -------
    xr.Dataset
        The Xarray Dataset object is the merged dataset of
        all forecast field include in the `file_list`. The
        Dataset object is lazily-loaded.
    """
    # getting the forecast/hindcast data
    mom6_dir = "/Datasets.private/regional_mom6/hindcast/"
    file_list = mom6_hindcast(mom6_dir)

    # static field
    ds_static = xr.open_dataset('/Datasets.private/regional_mom6/ocean_static.nc')
    ds_static = ds_static.set_coords(
        ['geolon','geolat',
         'geolon_c','geolat_c',
         'geolon_u','geolat_u',
         'geolon_v','geolat_v']
    )

    # merge the static field with the variables
    for file in file_list:
        if f'i{imonth}' in file and var in file :
            ds = xr.open_dataset(file)
    ds = xr.merge([ds_static,ds])

    return ds

def get_mom6_regrid() -> xr.Dataset:
    """
    Return the mom6 regridded grid hindcast/forecast field

    Returns
    -------
    xr.Dataset
        The Xarray Dataset object is the merged dataset of
        all forecast field include in the `file_list`. The
        Dataset object is lazily-loaded.
    """
    # getting the forecast/hindcast data
    mom6_dir = "/Datasets.private/regional_mom6/hindcast/regrid/"
    file_list = mom6_hindcast(mom6_dir)

    return xr.open_mfdataset(file_list)

def get_mom6_tercile_raw(
        imonth : int
) -> xr.Dataset:
    """return the mom6 quantile from the forecast

    Parameters
    ----------
    imonth : int
        The initialization month that generate the forecast

    Returns
    -------
    xr.Dataset
        A dataset that include the f_lowmid and f_midhigh value which 
        represent SST values at the boundaries between the terciles. 
        `f_lowmid` represent the boundary value between lower and middle
        tercile. `f_midhigh` represent the boundary value between middle
        and upper tercile. (the filename 'quantile' MIGHT be error naming)
    """
    # getting the forecast/hindcast data
    mom6_dir = "/Datasets.private/regional_mom6/tercile_calculation/"
    return xr.open_dataset(f'{mom6_dir}/forecast_quantiles_i{imonth:02d}.nc')

def get_init_fcst_time(
    iyear : int,
    imonth : int,
    bins : List[int],
) -> dict:
    """_summary_

    Parameters
    ----------
    iyear : int
        The year of initialization time
    imonth : int
        The month of initialization time
    bins : List[int]
        The `lead_bin` used to binned the leading month result
        example is `lead_bins = [0, 3, 6, 9, 12]` for four seasonal
        mean.

    Returns
    -------
    dict
        with two key-value pairs, 'init': initial_time and
        'fcst': mean forecasted during the binned period 
    """
    # get the cftime of initial time
    btime = cftime.datetime(iyear,imonth,1)

    # store the forecast time format based on all leadtime
    forecasttime = []
    period_length = bins[1]-bins[0]-1  # assuming the bins are equal space
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
    mean_forecasttime = [forecasttime[idx] for idx in bins[:-1]]

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

if __name__=='__main__':

    # user options
    ini_year = 2022
    ini_month = 3
    lead_season_index = 3
    varname = 'tos'

    # getting the regionl mask on mom grid
    ds_lme = get_mom6_regionl_mask()

    # getting the ocean mask on mom grid
    da_lmask = get_mom6_mask(mask='wet')

    # lazily-loaded the mom6 raw field
    ds_data = get_mom6_raw(imonth=ini_month,var=varname)

    # load variable to memory
    da_data = (
        ds_data
        .sel(init=f'{ini_year:04d}-{ini_month:02d}',drop=True).isel(init=0)
        [varname]
    )

    # setup lead bins to average during forecast lead time
    # (should match lead bins used for the historical data
    # that created the /Datasets.private/regional_mom6/tercile_calculation/historical_terciles.nc
    # [0, 3, 6, 9, 12] produces 3-month averages
    lead_bins = [0, 3, 6, 9, 12]
    lead_bin_label = np.arange(0,4)
    lead_bin_title = []

    # average the forecast over the lead bins
    da_binned = (
        da_data
        .groupby_bins('lead', lead_bins, labels=lead_bin_label, right=True)
        .mean('lead')
        .rename({'lead_bins': 'lead_bin'})
    )

    # find a normal distribution for each grid cell and lead bin
    # from the ensemble mean and standard deviation
    #  this is based on 1 initialization
    da_mean = da_binned.mean('member')
    da_std = da_binned.std('member')
    da_dist = normal(loc=da_mean, scale=da_std)

    # load the predetermined hindcast/forecast tercile value
    #  this is based on 30 years statistic 1993-2023
    ds_tercile = get_mom6_tercile_raw(imonth=ini_month)

    # average the forecast over the lead bins
    ds_tercile_binned = (
        ds_tercile
        .groupby_bins('lead', lead_bins, labels=lead_bin_label, right=True)
        .mean('lead')
        .rename({'lead_bins': 'lead_bin'})
    )

    # use single initialization's normal distribution
    # and pre-defined tercile value to find the
    # probability based on the single initialization
    # that correspond to the long-term statistic tercile value

    #---probability of lower tercile tail
    da_low_tercile_prob = xr.DataArray(
        da_dist.cdf(ds_tercile_binned['f_lowmid']),
        dims=da_mean.dims,
        coords=da_mean.coords
    )
    #---probability of upper tercile tail
    da_up_tercile_prob = 1 - xr.DataArray(
        da_dist.cdf(ds_tercile_binned['f_midhigh']),
        dims=da_mean.dims,
        coords=da_mean.coords
    )
    #---probability of between lower and upper tercile
    da_mid_tercile_prob = 1 - da_up_tercile_prob - da_low_tercile_prob

    da_tercile_prob = xr.concat(
        [da_low_tercile_prob,da_mid_tercile_prob,da_up_tercile_prob],
        pd.Index([-1,0,1],name="tercile")
    )

    # lower tercile max => negative
    # nomral tercile max => 0
    # upper tercile max => positive
    da_tercile_prob_max = (
        da_tercile_prob.idxmax(dim='tercile',fill_value=np.nan)*
        da_tercile_prob.max(dim='tercile')
    )

    # create dataset to store the tercile calculation
    ds_tercile_prob_max=xr.Dataset()
    ds_tercile_prob_max['tercile_prob_max'] = da_tercile_prob_max
    ds_tercile_prob_max['mask'] = da_lmask

    ############# plotting the max tercile probability
    # get time format
    dict_forecast_time = get_init_fcst_time(
        iyear=ini_year,
        imonth=ini_month,
        bins=lead_bins
    )

    # create the new colormap designed for tercile plot
    ncolor = 18
    RdYlBu = cm.get_cmap('RdYlBu', ncolor)
    newRdYlBu = RdYlBu(range(ncolor))
    white = np.array([255/255.,255/255.,255/255.,1.])
    grey = np.array([237./255., 231./255., 225./255.,1.])
    newRdYlBu[int((ncolor-1)/2),:] = grey
    newRdYlBu[int((ncolor-1)/2)+1,:] = grey
    newRdYlBu_r = newRdYlBu[::-1]
    newRdYlBu_r = ListedColormap(newRdYlBu_r)

    # plotting setting
    lon='geolon'
    lat='geolat'
    level = [-1,-0.9,-0.8,-0.7,-0.6,-0.5,-0.4,-0.33,0,0.33,0.40,0.5,0.6,0.7,0.8,0.9,1]
    clabel = []
    for ll in level:
        if ll>0:
            clabel.append(f"{ll*100:0.0f}%")
        elif ll<0:
            clabel.append(f"{-ll*100:0.0f}%")
        else:
            clabel.append("normal")
    cmap = newRdYlBu_r
    colorbar_labelname = 'SST tercile'
    fig=plt.figure(2,figsize=(6.5,7))
    ax2=fig.add_axes([0.12,0.1,1,0.8],projection=ccrs.PlateCarree(central_longitude=-60))
    ax2.set_aspect('auto')


    im=[
        ds_tercile_prob_max['tercile_prob_max']
        .isel(lead_bin=lead_season_index)
        .plot.pcolormesh(
            x=lon,
            y=lat,
            ax=ax2,
            levels=level,
            extend='neither',
            cmap=cmap,
            transform=ccrs.PlateCarree(central_longitude=0)
        )
    ]

    im[0].colorbar.remove()
    cbaxes=fig.add_axes([0.8,0.1,0.02,0.8])
    cbar=fig.colorbar(im[0],cax=cbaxes,orientation='vertical')
    cbar.set_ticks(level)
    cbar.set_ticklabels(clabel)
    cbar.ax.tick_params(labelsize=12,rotation=0)
    cbar.set_label(label=colorbar_labelname,size=12, labelpad=15)

    imm=(ds_tercile_prob_max['mask']
        .plot.pcolormesh(
            x=lon,
            y=lat,
            ax=ax2,
            levels=np.arange(0,1+1),
            cmap='grey',
            transform=ccrs.PlateCarree(central_longitude=0)
        )
    )
    imm.colorbar.remove()

    abovearrow = ax2.text(1.31, 0.65, "Upper tercile %",color='white',transform=ax2.transAxes,
                ha="center", va="bottom", rotation=90, size=12,
                bbox=dict(boxstyle="rarrow,pad=0.5",
                        fc="#EE9322", ec="#E55604", lw=1))

    belowarrow = ax2.text(1.31, 0.34, "Lower tercile %",color='white',transform=ax2.transAxes,
                ha="center", va="top", rotation=90, size=12,
                bbox=dict(boxstyle="larrow,pad=0.5",
                        fc="#6499E9", ec="#2E4374", lw=1))

    title =(
        f'Forecast {dict_forecast_time["fcsts"][lead_season_index]}\n'+
        f'{dict_forecast_time["init"]} Initialized'
    )
    ax2.set_title(
        title,
        fontsize=16
    )

    fig.savefig('/home/chsu/regional_mom/figures/tercile.png')
