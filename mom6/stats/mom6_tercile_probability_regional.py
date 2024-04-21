#!/usr/bin/env python

"""
This is the script to genearte the regional tercile probability plot based on the 
forecast(hindcast) that is generated by Andrew Ross at GFDL.

current code is implemented on the raw grid only

The code is also applied on the web regionl tercile probability visualization


"""
import os
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.colors import ListedColormap
from matplotlib import cm
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from scipy.stats import norm as normal
from mom6.mom6_module import mom6_process as mp


if __name__=='__main__':

    # user options
    ini_year = 2022
    ini_month = 3
    lead_season_index = 3
    varname = 'tos'
    data_grid = 'raw'
    region = 'MAB'

    # getting the regionl mask on mom grid
    ds_lme = mp.MOM6Static.get_mom6_regionl_mask()

    # getting the ocean mask on mom grid
    da_lmask = mp.MOM6Static.get_mom6_mask(mask='wet',grid=data_grid)

    # loaded the mom6 raw field for single initialization
    mom6Forecast = mp.MOM6Forecast(iyear=ini_year,imonth=ini_month,var=varname,grid=data_grid)
    ds_data = mom6Forecast.get_mom6()

    # load variable to memory (remove the init dimension)
    da_data = ds_data[varname].isel(init=0)

    # Area weighted average to the specific region
    da_data = (
        (ds_lme[region]*da_data*ds_lme['areacello']).sum(dim=['xh','yh'])/
        (ds_lme[region]*ds_lme['areacello']).sum(dim=['xh','yh'])
    )   # if regrid of other stagger grid this need to be changed

    # setup lead bins to average during forecast lead time
    # (should match lead bins used for the historical data
    # that created the /Datasets.private/regional_mom6/tercile_calculation/historical_terciles.nc
    # [0, 3, 6, 9, 12] produces 3-month averages
    lead_bins = [0, 3, 6, 9, 12]
    lead_bin_label = np.arange(0,len(lead_bins)-1)
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
    da_tercile = mom6Forecast.get_mom6_tercile_regional().sel(region=region)

    # average the forecast over the lead bins
    da_tercile_binned = (
        da_tercile
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
        da_dist.cdf(da_tercile_binned['f_lowmid']),
        dims=da_mean.dims,
        coords=da_mean.coords
    )
    #---probability of upper tercile tail
    da_up_tercile_prob = 1 - xr.DataArray(
        da_dist.cdf(da_tercile_binned['f_midhigh']),
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
    ds_tercile_prob=xr.Dataset()
    ds_tercile_prob['tercile_prob'] = da_tercile_prob
    ds_tercile_prob['tercile_prob_max'] = da_tercile_prob_max
    ds_tercile_prob['mask'] = ds_lme[region]

    ############# plotting the max tercile probability
    # get time format
    dict_forecast_time = mom6Forecast.get_init_fcst_time()

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
    if data_grid == 'raw':
        lon='geolon'
        lat='geolat'
    elif data_grid == 'regrid':
        lon='lon'
        lat='lat'
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
        (ds_tercile_prob['tercile_prob_max']*ds_lme[region])
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

    proj_dir = os.path.dirname(
        os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__)
        )
        )
    )
    fig.savefig(f'{proj_dir}/figures/tercile_regional.png')
