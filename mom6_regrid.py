"""
This script is designed to do batch regridding of the 
regional mom6 output

The regridding is using the xesmf package with the bilinear method
there are other more conservative way to doing the regriddeding 
https://xesmf.readthedocs.io/en/stable/notebooks/Compare_algorithms.html 

Current regridded product is based on the following keys
1. The number of grid is close or similar to the original 
   curvilinear grid.
2. The regridded product should cover the maximum/minimum lon lat
   range that is provided by the original curvilinear grid

Based on the above mentioned keys
1. the number of grid in lon,lat is the same as xh,yh
2. the linspace range of lon lat is defined by 
   max(geolon/geolat) and min(geolon/geolat)

"""
# %%
import sys
import warnings
import numpy as np
import xarray as xr
import xesmf as xe
from dask.distributed import Client
warnings.simplefilter("ignore")

def mom6_encoding_attr(
        ds_data_ori,
        ds_data,
        var_names=None,
        dataset_name='regional mom6 regrid'
    ):
    """
    This function is designed for creating attribute and netCDF encoding
    for the regional mom6 regrid file format.

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
        ds_data[var_name].attrs = ds_data_ori[var_name].attrs
        ds_data[var_name].encoding = ds_data_ori[var_name].encoding
        ds_data[var_name].encoding['complevel'] = 2

    return ds_data

def mom6_hist_run(parent_dir):
    """
    Create list of files to be able to be opened 
    by Xarray.


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
    hpoint_file_list = [f"{parent_dir}{file}" for file in hpoint_file_list]

    # T point that is the same as h point
    tpoint_file_list = [
        "ice_monthly.199301-201912.siconc.nc"
    ]
    tpoint_file_list = [f"{parent_dir}{file}" for file in tpoint_file_list]

    all_file_list = hpoint_file_list+tpoint_file_list

    return all_file_list

def mom6_hindcast(parent_dir):
    """
    Create list of files to be able to be opened 
    by Xarray.


    """
    tob_files = [f"tob_forecasts_i{mon}.nc" for mon in range(3,13,3)]
    tos_files = [f"tos_forecasts_i{mon}.nc" for mon in range(3,13,3)]

    # h point list
    hpoint_file_list = (
        tob_files+
        tos_files
    )

    hpoint_file_list = [f"{parent_dir}{file}" for file in hpoint_file_list]

    all_file_list = hpoint_file_list

    return all_file_list

# %%
if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # check argument exist
    if len(sys.argv) < 2:
        print("Usage: python mom6_regrid.py hist/fcst")
        sys.exit(1)
    # %%
    if sys.argv[1] == 'hist':
        # file location and name
        mom6_dir = "/Datasets.private/regional_mom6/hist_run/"
        file_list = mom6_hist_run(mom6_dir)
    elif sys.argv[1] == 'fcst':
        mom6_dir = "/Datasets.private/regional_mom6/hindcast/"
        file_list = mom6_hindcast(mom6_dir)

    # %%
    # static field
    ds_static = xr.open_dataset('/Datasets.private/regional_mom6/ocean_static.nc')
    ds_static = ds_static.set_coords(
        ['geolon','geolat',
         'geolon_c','geolat_c',
         'geolon_u','geolat_u',
         'geolon_v','geolat_v']
    )
    ds_static['geolon'] = ds_static['geolon']+360.
    # for xesmf to rename
    ds_static = ds_static.rename({'geolon':'lon','geolat':'lat'})

    # %%
    # Create longitude and latitude arrays (e.g., 1D arrays)
    x = np.linspace(
        ds_static.lon.min(),
        ds_static.lon.max(),
        len(ds_static.xh)-1
    )    # Example longitude values
    y = np.linspace(
        ds_static.lat.min(),
        ds_static.lat.max(),
        len(ds_static.yh)-1
    )    # Example latitude values

    # Create a dummy data variable (optional, can be empty)
    ds = xr.open_dataset(file_list[0],chunks={})
    if sys.argv[1] == 'hist':
        data = xr.DataArray(
            data=None,
            coords={'lon': x, 'lat': y},
            dims=('lon', 'lat')
        )  
    elif sys.argv[1] == 'fcst':
        data = xr.DataArray(
            data=None,
            coords={'lon': x, 'lat': y, "member": ds.member, "init": ds.init, "lead": ds.lead},
            dims=('lon', 'lat', 'member', 'init', 'lead')
        )
     
    # Create an xarray dataset with empty dataarray
    ds_regrid = xr.Dataset({'var': data})

    # %%
    # use xesmf to create regridder
    # !!!! regridded only suited for geolon and geolat to x and y
    regridder = xe.Regridder(ds_static, ds_regrid, "bilinear", unmapped_to_nan=True)

    # %%
    # open each file and regrid
    for file in file_list :
        ds = xr.open_dataset(file)

        if "siconc" in file:
            # cover dim name to the corrected name for Tpoint(error in the model)
            ds = ds.rename({'xT':'xh','yT':'yh'})

        # merge with static field
        ds = xr.merge([ds,ds_static])

        if sys.argv[1] == 'hist':
            varname = file.split(".")[-2]
            ds_regrid = xr.Dataset()
            # perform regrid for each field
            ds_regrid[varname] = regridder(ds[varname])

        elif sys.argv[1] == 'fcst':
            varname = file.split("/")[-1].split("_")[0]
            ds_regrid = xr.Dataset()
            # perform regrid for each field
            ds_regrid[varname] = regridder(ds[varname])
            ds_regrid[f"{varname}_anom"] = regridder(ds[f"{varname}_anom"])

        # output the netcdf file
        print(f'output {mom6_dir}/regrid/{file[len(mom6_dir):]}')
        mom6_encoding_attr(
                ds,
                ds_regrid,
                var_names=list(ds_regrid.keys()),
                dataset_name='regional mom6 regrid'
            )
        try:
            ds_regrid.to_netcdf(f'{mom6_dir}/regrid/{file[len(mom6_dir):]}',mode='w')
        except PermissionError:
            print(f'{mom6_dir}/regrid/{file[len(mom6_dir):]} is used by other scripts' )

