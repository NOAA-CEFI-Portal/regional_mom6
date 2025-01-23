"""
This script is designed to do batch regridf of the 
regional mom6 output usig the new mom6_read module

The regridding is using the xesmf package with the bilinear method
there are other more conservative way to doing the regriddeding 
https://xesmf.readthedocs.io/en/stable/notebooks/Compare_algorithms.html 


"""
import os
import sys
import warnings
import xarray as xr
from dask.distributed import Client
from mom6_rotate_batch import output_processed_data
from mom6.data_structure import portal_data
from mom6.mom6_module.mom6_read import AccessFiles
from mom6.mom6_module.mom6_regrid import Regridding
from mom6.mom6_module.mom6_export import mom6_regular_grid_encode_attr
from mom6.data_structure.batch_preprocess_hindcast import load_json

warnings.simplefilter("ignore")

def regrid_batch(dict_json:dict)->xr.Dataset:
    """perform the batch regridding of the mom6 output

    Parameters
    ----------
    dict_json : dict
        dictionary that contain the constant setting in json

    Returns
    -------
    xr.Dataset
        regridded dataset
    """

    local_top_dir=dict_json['local_top_dir']
    region=dict_json['region']
    subdomain=dict_json['subdomain']
    experiment_type=dict_json['experiment_type']
    output_frequency=dict_json['output_frequency']
    grid_type=dict_json['grid_type']
    release=dict_json['release']
    data_source=dict_json['data_source']

    # determine the data path
    output_cefi_rel_path = portal_data.DataPath(
        region=region,
        subdomain=subdomain,
        experiment_type=experiment_type,
        output_frequency=output_frequency,
        grid_type=dict_json['output']['cefi_grid_type'],
        release=dict_json['release']
    ).cefi_dir

    output_dir = os.path.join(local_top_dir,output_cefi_rel_path)

    # Check if the release directory already exists
    if not os.path.exists(output_dir):
        print(f"Creating release folder in last level: {output_dir}")
        # Create the directory
        os.makedirs(output_dir, exist_ok=True)
    else:
        print(f"release folder already exists: {output_dir}")

    # get all files in the experiment
    local_access = AccessFiles(
        local_top_dir=local_top_dir,
        region=region,
        subdomain=subdomain,
        experiment_type=experiment_type,
        output_frequency=output_frequency,
        grid_type=grid_type,
        release=release,
        data_source=data_source
    )

    allfile_list = local_access.get()
    statics = local_access.get(variable='ocean_static')

    # prepare static data
    ds_static = xr.open_dataset(statics[0]).drop_vars('time') # time dim not needed

    # loop through all file in the original path
    for file in allfile_list:
        # try to avoid the static file
        if 'static' not in file:
            # open the file
            ds_var = xr.open_dataset(file,chunks={})
            varname = ds_var.attrs['cefi_variable']

            # create new filename based on original filename
            filename = ds_var.attrs['cefi_filename']
            filename_seg = filename.split('.')
            grid_type_index = filename_seg.index('raw')
            filename_seg[grid_type_index] = dict_json['output']['cefi_grid_type']
            new_filename = '.'.join(filename_seg)

            # find if new file name already exist
            new_file = os.path.join(output_dir,new_filename)
            if os.path.exists(new_file):
                print(f"{new_file}: already exists. skipping...")
            else:
                # find the variable dimension info (for chunking)
                print(f"processing {new_file}")

                # get xname and yname (need expand if there are other grids)
                dims = list(ds_var.dims)
                if all(dim in dims for dim in ['xq', 'yh']):
                    xname = 'geolon_u'
                    yname = 'geolat_u'
                    xdimorder = dims.index('xq')
                    ydimorder = dims.index('yh')
                elif all(dim in dims for dim in ['xh', 'yh']):
                    xname = 'geolon'
                    yname = 'geolat'
                    xdimorder = dims.index('xh')
                    ydimorder = dims.index('yh')
                elif all(dim in dims for dim in ['xh', 'yq']):
                    xname = 'geolon_v'
                    yname = 'geolat_v'
                    xdimorder = dims.index('xh')
                    ydimorder = dims.index('yq')
                else:
                    raise ValueError("Unknown grid (need implementations)")

                # merge static field to include lon lat info
                ds_var = xr.merge([ds_var,ds_static],combine_attrs='override')

                # call regridding class
                class_regrid = Regridding(ds_var,varname,xname,yname)
                nx = len(ds_var[dims[xdimorder]])
                ny = len(ds_var[dims[ydimorder]])
                # perform regridding
                ds_regrid = class_regrid.regrid_regular(nx,ny)

                # copy the encoding and attributes
                ds_regrid = mom6_regular_grid_encode_attr(ds_var,ds_regrid,var_names=[varname])

                # redefine new global attribute
                # global attributes
                ds_regrid.attrs['cefi_rel_path'] = output_dir
                ds_regrid.attrs['cefi_filename'] = new_filename
                ds_regrid.attrs['cefi_grid_type'] = dict_json['output']['cefi_grid_type']

    return ds_regrid


if __name__=="__main__":

    client = Client(processes=False,memory_limit='500GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 1:
        print("Usage: python mom6_regrid_batch.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]

    current_location = os.path.dirname(os.path.abspath(__file__))
    log_name = sys.argv[1].split('.')[0]+'.log'
    log_filename = os.path.join(current_location,log_name)

    with open(log_filename, "w", encoding='utf-8') as log_file:
        sys.stdout = log_file
        sys.stderr = log_file

        # Load the settings
        dict_json1 = load_json(json_setting,json_path=current_location)

        # preprocessing the file to cefi format
        ds_regrid1 = regrid_batch(dict_json1)

        # output the processed data
        output_processed_data(ds_regrid1,top_dir=dict_json1['local_top_dir'],dict_json_output=dict_json1['output'])

    # Reset to default after exiting the context manager
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
