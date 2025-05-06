"""
This script is designed to do batch rotate of the 
regional mom6 output

The regridding is using the xesmf package with the bilinear method
there are other more conservative way to doing the regriddeding 
https://xesmf.readthedocs.io/en/stable/notebooks/Compare_algorithms.html 


"""
import os
import sys
import glob
import warnings
import xarray as xr
from dask.distributed import Client
from mom6 import DATA_PATH
from mom6.mom6_module.mom6_io import MOM6Static,MOM6Misc,MOM6Historical
from mom6.mom6_module.mom6_vector_rotate import VectorRotation

warnings.simplefilter("ignore")

if __name__=="__main__":

    client = Client(processes=False,memory_limit='150GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    chunk_time = 50

    # check argument exist
    if len(sys.argv) < 2:
        print("Usage: python mom6_rotate_single.py <hist/fcst> <u varnames> <v varnames>")
        sys.exit(1)

    # dictionary for input argument to dir name
    dict_dir = {
        'hist': 'hist_run/',
        'fcst': 'forecast/'
    }

    # store u v names
    var_list = []
    mom6_dir = os.path.join(DATA_PATH,dict_dir[sys.argv[1]])
    for var in sys.argv[2:]:
        var_list.append(var)
        if 'u' in var:
            u_name = var
            ufile_list = glob.glob(f'{mom6_dir}/*{var}.nc')
        elif 'v' in var:
            v_name = var
            vfile_list = glob.glob(f'{mom6_dir}/*{var}.nc')

    # for historical run
    if sys.argv[1] == 'hist':
        ClassU = MOM6Historical(
            var=u_name,
            data_relative_dir=dict_dir[sys.argv[1]],
            static_relative_dir='static/',
            grid='raw',
            source='local',
            chunks={'time':100,'xq':-1,'yh':-1}
        )
        ds_u = ClassU.get_all()

        ClassV = MOM6Historical(
            var=v_name,
            data_relative_dir=dict_dir[sys.argv[1]],
            static_relative_dir='static/',
            grid='raw',
            source='local',
            chunks={'time':100,'xh':-1,'yq':-1}
        )
        ds_v = ClassV.get_all()

        # get source file location
        file_loc_u = ds_u[u_name].encoding['source']
        file_loc_u_list = file_loc_u.split('/')
        parent_data_path_u = f'/{os.path.join(*file_loc_u_list[:-1])}/'
        ufilename = file_loc_u_list[-1]

        file_loc_v = ds_v[v_name].encoding['source']
        file_loc_v_list = file_loc_v.split('/')
        parent_data_path_v = f'/{os.path.join(*file_loc_v_list[:-1])}/'
        vfilename = file_loc_v_list[-1]

        if parent_data_path_u != parent_data_path_v:
            raise IOError('the input u and v components are not from the same level')
        else :
            parent_data_path = parent_data_path_u

    # for rotation matrix
    ClassRotate = MOM6Static
    ds_rotate = ClassRotate.get_rotate(data_relative_dir='static/')

    # setup the rotation class
    ClassRotate = VectorRotation(ds_u,u_name,ds_v,v_name,ds_rotate)

    # perform lazy rotate
    dict_uv = ClassRotate.generate_true_uv()

    ds_u_true = xr.Dataset()
    ds_v_true = xr.Dataset()
    ds_u_true[f'{u_name}'] = dict_uv['u']
    ds_v_true[f'{v_name}'] = dict_uv['v']

    MOM6Misc.mom6_encoding_attr(
        ds_u,
        ds_u_true,
        var_names=list(ds_u_true.keys()),
        dataset_name='regional mom6 vector rotate'
    )
    ds_u_true = ds_u_true.drop_vars(['lon','lat'])
    ori_long_name =  ds_u[u_name].attrs['long_name']
    ds_u_true[u_name].attrs['long_name'] = f"Rotated {ori_long_name}"
    ds_u_true[u_name].encoding = {
        'zlib': True,
        'szip': False,
        'zstd': False,
        'bzip2': False,
        'blosc': False,
        'shuffle': True,
        'complevel': 3,
        'fletcher32': False,
        'contiguous': False,
        'chunksizes': [chunk_time,1000,1000]
    }
    ds_u_true = ds_u_true.rename({u_name : f'{u_name}_rotate'})

    MOM6Misc.mom6_encoding_attr(
        ds_v,
        ds_v_true,
        var_names=list(ds_v_true.keys()),
        dataset_name='regional mom6 vector rotate'
    )
    ds_v_true = ds_v_true.drop_vars(['lon','lat'])
    ori_long_name =  ds_v[v_name].attrs['long_name']
    ds_v_true[v_name].attrs['long_name'] = f"Rotated {ori_long_name}"
    ds_v_true[v_name].encoding = {
        'zlib': True,
        'szip': False,
        'zstd': False,
        'bzip2': False,
        'blosc': False,
        'shuffle': True,
        'complevel': 3,
        'fletcher32': False,
        'contiguous': False,
        'chunksizes': [chunk_time,1000,1000]
    }
    ds_v_true = ds_v_true.rename({v_name : f'{v_name}_rotate'})

    # output the netcdf file
    if sys.argv[1] == 'hist':
        print(f'output {parent_data_path}{ufilename[:-3]}_rotate.nc')
        try:
            ds_u_true.to_netcdf(f'{parent_data_path}{ufilename[:-3]}_rotate.nc',mode='w')
        except PermissionError as e:
            raise PermissionError(
                f'{parent_data_path}{ufilename[:-3]}_rotate.nc is used by other scripts'
            ) from e

        print(f'output {parent_data_path}{vfilename[:-3]}_rotate.nc')
        try:
            ds_v_true.to_netcdf(f'{parent_data_path}{vfilename[:-3]}_rotate.nc',mode='w')
        except PermissionError as e:
            raise PermissionError(
                f'{parent_data_path}{vfilename[:-3]}_rotate.nc is used by other scripts'
            ) from e
    # output for forecast option still need implementation
