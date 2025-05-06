from typing import List
import numpy as np
import xarray as xr

def mom6_encode_attr(
    ds_data_ori : xr.Dataset,
    ds_data : xr.Dataset,
    var_names : List[str] = None
):
    """
    This function is designed for creating attribute and netCDF encoding
    for the regular grid (regrid) regional mom6 file format.

    Duplicate the entire attribute and encoding from the original dataset!!!
    anything modifing the attr and encoding will be ignored.

    Run this before modifying the attr and encoding for the new dataset.

    Parameters
    ----------
    ds_data_ori : xr.Dataset
        original dataset
    ds_data : xr.Dataset
        new output regridded dataset
    var_name : string
        var name in the dataset

    Returns
    -------
    ds_data : xr.Dataset
        new output regridded dataset with attr and encoding setup.
    
    Raises
    ------

    """
    # get the list of dims and variables
    dims = list(ds_data.dims)

    if var_names is None:
        var_names = []

    # global attrs and encoding
    ds_data.attrs = ds_data_ori.attrs
    ds_data.encoding = ds_data_ori.encoding

    try:
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
    except KeyError:
        print('lon dimension not in the dataset')

    try:
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
    except KeyError:
        print('lat dimension not in the dataset')

    # copy original attrs and encoding for dims
    for dim in dims:
        if dim not in ['lon','lat']:
            try:
                if ds_data[dim].attrs == {}:
                    ds_data[dim].attrs = ds_data_ori[dim].attrs
                    ds_data[dim].encoding = ds_data_ori[dim].encoding
                    ds_data[dim].encoding['complevel'] = 2
            except KeyError:
                print(f'{dim} dimension not duplicated')

    # copy original attrs and encoding for variables
    for var_name in var_names:
        try:
            # no attrs in the new variable
            if ds_data[var_name].attrs == {}:
                ds_data[var_name].attrs = ds_data_ori[var_name].attrs
                ds_data[var_name].encoding = ds_data_ori[var_name].encoding
                print(f'{var_name} duplicated')
            # new variable has existing attrs duplicate missing attrs from original data attrs
            else:
                ds_data[var_name].encoding = ds_data_ori[var_name].encoding
                new_attrs = list(ds_data[var_name].attrs.keys())
                ori_attrs = list(ds_data_ori[var_name].attrs.keys())
                for attr in ori_attrs:
                    if attr not in new_attrs:
                        ds_data[var_name].attrs[attr] = ds_data_ori[var_name].attrs[attr]
                print(f'{var_name} duplicated missing attrs from original data attrs')

        except KeyError:
            print(f'new variable name {var_name} not in the original dataset')
            ds_data[var_name].encoding['complevel'] = 2

    return ds_data