"""
This script is designed to calculate the regionally 
avereaged tercile value (EPU) based on the raw gridded
regional MOM6 forecast output

regrid product need to have EPU mask avaialble if one 
want to add to the script

"""
# %%
import os
import sys
import glob
import warnings
import xarray as xr
from dask.distributed import Client
from mom6 import DATA_PATH
from mom6.mom6_module.mom6_io import MOM6Static,MOM6Misc

warnings.simplefilter("ignore")

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # check argument exist
    if len(sys.argv) < 3:
        print("Usage: python mom6_tercile_regional.py VARNAME GRIDTYPE")
        sys.exit(1)
    else:
        varname = sys.argv[1]
        grid = sys.argv[2]     # regrid or raw

    # data locations
    if grid == 'raw':
        MOM6_DIR = os.path.join(DATA_PATH,"hindcast/")
        MOM6_TERCILE_DIR = os.path.join(DATA_PATH,"tercile_calculation/")
    else:
        print("Usage: python mom6_tercile_regional.py VARNAME GRIDTYPE")
        raise NotImplementedError('GRIDTYPE can only be "raw"')

    file_list = glob.glob(MOM6_DIR+'/*.nc')
    var_file_list = []
    for file in file_list :
        if varname in file :
            var_file_list.append(file)

    # open data file
    for file in var_file_list :
        ds = xr.open_dataset(file)
        ds_mask = MOM6Static.get_regionl_mask('masks/')

        # apply mask
        da = ds[varname]
        da_area = ds_mask['areacello']
        reg_tercile_list = []
        reg_list = []
        for region in list(ds_mask.keys()):
            if region != 'areacello':
                # calculate the regional area-weighted mean
                da_mask = ds_mask[region]
                
                da = (
                    (da*da_mask*da_area).sum(dim=['xh','yh'])/
                    (da_mask*da_area).sum(dim=['xh','yh'])
                )   # if regrid of other stagger grid this need to be changed

                # calculate the tercile value
                da_tercile = (
                    da
                    .stack(allens=('init','member'))
                    .quantile(
                        [1./3.,2./3.],
                        dim='allens',
                        keep_attrs=True
                    )
                )

                # store all regional averaged tercile
                reg_tercile_list.append(da_tercile)
                reg_list.append(region)

        # concat all regional tercile to one DataArray
        da_tercile = xr.concat(reg_tercile_list,dim='region')
        da_tercile['region'] = reg_list

        # store the DataArray to Dataset with tercile seperated
        ds_tercile = xr.Dataset()
        ds_tercile.attrs = ds.attrs
        ds_tercile['f_lowmid'] = da_tercile.isel(quantile=0)
        ds_tercile['f_midhigh'] = da_tercile.isel(quantile=1)
        ds_tercile = ds_tercile.drop_vars('quantile')

        # output the netcdf file
        print(f'output {MOM6_TERCILE_DIR}{file[len(MOM6_DIR):-6]}tercile_{file[-6:-3]}.region.nc')
        MOM6Misc.mom6_encoding_attr(
                ds,
                ds_tercile,
                var_names=list(ds_tercile.keys()),
                dataset_name='regional mom6 tercile'
            )
        try:
            ds_tercile.to_netcdf(f'{MOM6_TERCILE_DIR}{file[len(MOM6_DIR):-6]}tercile_{file[-6:-3]}.region.nc',mode='w')
        except PermissionError:
            print(f'{MOM6_TERCILE_DIR}{file[len(MOM6_DIR):-6]}tercile_{file[-6:-3]}.region.nc is used by other scripts' )
