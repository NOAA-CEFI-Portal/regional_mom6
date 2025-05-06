#!/usr/bin/env python

'''
Regional GLORYS CPI mask
- Using the entire avialable GLORYS period for better climatology (1993-2022)

Criteria for determine the Cold Pool Index (CPI) mask
- define the Cold Pool Domain
    - MAB domain and between 38N-41.5N and between 75W-68.5W
    - between 20m to 200m isobath
    - average bottom temperature was cooler than 10°C (and > 6?) between June and September from 1959 to 2022
    - slicing off north of 41N or west of 70W? (southern flank of Georges Bank)

'''
import numpy as np
import xarray as xr
import xesmf as xe
from mom6.mom6_module.deprecated import mom6_io

if __name__ == '__main__':
    
    glorys_dir = 'folder/with/Glorys/data'

    ### Glorys IO
    glorys_file = f'{glorys_dir}/glorys/bottomT.mon.mean.199301-202012.nc'
    glorys_static_file = f'{glorys_dir}/glorys/GLO-MFC_001_030_mask_bathy.nc'
    ds = xr.open_mfdataset([glorys_file,glorys_static_file],chunks={'time':1})
    ds = ds.drop_vars('mask')

    ### 20~200 meter mask (Glorys)
    # based on bathymatry
    da_mask_depth = ds.deptho.where((ds.deptho>20.)&(ds.deptho<200.),other=np.nan)
    da_mask_depth = xr.where(da_mask_depth.notnull(),x=1,y=np.nan)

    ### Average of bottom temperature mask (Glorys)
    # - during June-September
    # - annual mean during June-September
    # - long-term mean of annual mean during 1993-2022
    # - long-term mean lower than 10 degC
    da_bottomT_Jun2Sep = ds.bottomT.where(
        (ds['time.month']>=6)&
        (ds['time.month']<=9),
        drop=True
    )
    da_bottomT_ann_ltm = (
        da_bottomT_Jun2Sep
        .groupby(da_bottomT_Jun2Sep['time.year'])
        .mean(dim='time')
        .mean(dim='year')
    ).compute()

    da_mask_bottomT_ann = xr.where(da_bottomT_ann_ltm<10, x=1, y=np.nan)


    ### MAB with regional crop from MOM6 to Glorys
    # can be improved with shapefile of the MAB on the GLORYS directly
    # - MAB
    # - remap to GLORYS
    # - regional crop

    ds_mask = mom6_io.MOM6Static.get_regionl_mask('masks/')
    # use xesmf to create regridder
    # !!!! regridded only suited for geolon and geolat to x and y
    # regrid MAB mask from MOM6 to GLORYS
    regridder = xe.Regridder(
        ds_mask.rename({'geolon':'lon','geolat':'lat'}),
        ds,
        "bilinear",
        unmapped_to_nan=True
    )

    # perform regrid
    # https://pangeo-xesmf.readthedocs.io/en/latest/notebooks/Masking.html#Adaptive-masking
    da_mask_regrid = regridder(ds_mask['MAB'], skipna=True, na_thres=0.25).compute()
    
    # regional crop
    #  GLORYS data on 0-360
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

    ### combine all three criteria
    da_mask_total_ann = da_mask_regrid_crop*da_mask_bottomT_ann*da_mask_depth

    ### Mask output (GLORYS grid)
    ds_cpi_mask = xr.Dataset()
    ds_cpi_mask['CPI_mask'] = da_mask_total_ann
    da_mask_total_ann.attrs['units'] = '1 over cold pool, fillvalue outside'
    da_mask_total_ann.attrs['long_name'] = 'cold pool index mask based on only GLORYS'
    ds_cpi_mask.attrs['desc'] = (
        'Mask is on GLORYS grid. '+
        'Cold pool index need to be calculated on the GLORYS grid. '+
        'Following Ross et al., 2023 approach, user should regrid data to the mask grid.'
    )
    ds_cpi_mask.to_netcdf(f'{glorys_dir}/masks/cpi_mask.nc')
