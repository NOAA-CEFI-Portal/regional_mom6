"""
This script is designed to do batch regridf of the 
regional mom6 output usig the new mom6_read module

The regridding is using the xesmf package with the bilinear method
there are other more conservative way to doing the regriddeding 
https://xesmf.readthedocs.io/en/stable/notebooks/Compare_algorithms.html 
"""
import os
import sys
import logging
import warnings
import xarray as xr
from dask.distributed import Client
from mom6_rotate_batch import output_processed_data
from mom6.data_structure import portal_data
from mom6.mom6_module.mom6_read import AccessFiles
from mom6.mom6_module.mom6_regrid import Regridding
from mom6.mom6_module.mom6_export import mom6_encode_attr
from mom6.data_structure.batch_preprocess_hindcast import load_json
from mom6.data_structure.portal_data import DataStructure

warnings.simplefilter("ignore")

def regrid_batch(dict_json:dict,logger_object)->xr.Dataset:
    """perform the batch regridding of the mom6 output

    TODO: dealing with the ice_month static field and 
    also the ice_month field

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
        logger_object.info(f"Creating release folder in last level: {output_dir}")
        # Create the directory
        os.makedirs(output_dir, exist_ok=True)
    else:
        logger_object.info(f"release folder already exists: {output_dir}")

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
    ice_statics = local_access.get(variable='ice_monthly')

    # prepare static data
    try:
        ds_static = xr.open_dataset(statics[0]).drop_vars('time') # time dim not needed
    except ValueError:
        ds_static = xr.open_dataset(statics[0])
    ds_static_ice = xr.open_dataset(ice_statics[0])

    # loop through all file in the original path
    for file in allfile_list:
        # try to avoid the static file
        if 'static' not in file:
            # open the file
            with xr.open_dataset(file, chunks={}) as ds_var:
                varname = ds_var.attrs['cefi_variable']

                # create new filename based on original filename
                filename = ds_var.attrs['cefi_filename']
                filename_seg = filename.split('.')
                grid_type_index = filename_seg.index('raw')
                filename_seg[grid_type_index] = dict_json['output']['cefi_grid_type']
                new_filename = '.'.join(filename_seg)

                # find if new file name already exist
                new_file = os.path.join(output_dir, new_filename)
                if os.path.exists(new_file):
                    logger_object.info(f"{new_file}: already exists. skipping...")
                else:
                    # find the variable dimension info (for chunking)
                    logger_object.info(f"processing {new_file}")

                    # get xname and yname (need expand if there are other grids)
                    dims = list(ds_var.dims)
                    if all(dim in dims for dim in ['xq', 'yh']):
                        xname = 'geolon_u'
                        yname = 'geolat_u'
                        xdimorder = dims.index('xq')
                        ydimorder = dims.index('yh')
                        # stop regrid due to u grid need rotation first
                        logger_object.info("Skipping file due to UGRID need rotation first")
                        continue
                    elif all(dim in dims for dim in ['xh', 'yh']):
                        # currently only support tracer grid regridding
                        xname = 'geolon'
                        yname = 'geolat'
                        xdimorder = dims.index('xh')
                        ydimorder = dims.index('yh')
                        # merge static field to include lon lat info
                        ds_var = xr.merge([ds_var, ds_static], combine_attrs='override')
                    elif all(dim in dims for dim in ['xh', 'yq']):
                        xname = 'geolon_v'
                        yname = 'geolat_v'
                        xdimorder = dims.index('xh')
                        ydimorder = dims.index('yq')
                        # stop regrid due to v grid need rotation first
                        logger_object.info("Skipping file due to VGRID need rotation first")
                        continue
                    elif all(dim in dims for dim in ['xT', 'yT']):
                        # ice month static field replace
                        # currently only support ice tracer grid regridding
                        xname = 'GEOLON'
                        yname = 'GEOLAT'
                        xdimorder = dims.index('xT')
                        ydimorder = dims.index('yT')
                        # merge static field to include lon lat info
                        ds_var = xr.merge([ds_var, ds_static_ice], combine_attrs='override')
                    else:
                        try:
                            raise ValueError("Unknown grid (need implementations)")
                        except ValueError as e:
                            logger_object.info(f"Skipping file due to error: {e}")
                            continue

                    # call regridding class
                    class_regrid = Regridding(ds_var, varname, xname, yname)
                    nx = len(ds_var[dims[xdimorder]])
                    ny = len(ds_var[dims[ydimorder]])
                    # perform regridding
                    ds_regrid = class_regrid.regrid_regular(nx, ny)
                    # forecast/reforecast files has two varname in one single file
                    try:
                        class_regrid_anom = Regridding(ds_var, varname + '_anom', xname, yname)
                        # perform regridding
                        ds_regrid_anom = class_regrid_anom.regrid_regular(nx, ny)
                        ds_regrid = xr.merge([ds_regrid, ds_regrid_anom])
                    except KeyError:
                        pass

                    # copy the encoding and attributes
                    ds_regrid = mom6_encode_attr(ds_var, ds_regrid, var_names=[varname])

                    # redefine new global attribute
                    # global attributes

                    # create new cefi_rel_path based on original cefi_rel_path
                    filepath = ds_var.attrs['cefi_rel_path']
                    filepath_seg = filepath.split('.')

                    # Change 'raw' to 'regrid'
                    for i, element in enumerate(filepath_seg):
                        if element == 'raw':
                            filepath_seg[i] = dict_json['output']['cefi_grid_type']

                    new_cefi_rel_path = '.'.join(filepath_seg)

                    ds_regrid.attrs['cefi_rel_path'] = new_cefi_rel_path
                    ds_regrid.attrs['cefi_filename'] = new_filename
                    ds_regrid.attrs['cefi_grid_type'] = dict_json['output']['cefi_grid_type']

                    # output the processed data
                    output_processed_data(
                        ds_regrid,
                        top_dir=dict_json['local_top_dir'],
                        dict_json_output=dict_json['output']
                    )
        # regridding ocean static variables (one variable at a time)
        elif 'ocean_static.nc' in file:
            try:
                # only regrid static field will have this variable in json file
                logger_object.info(f"output static field : {dict_json['static_variable']}")
                # redefine output data path for static regrid
                output_cefi_rel_path = portal_data.DataPath(
                    top_directory=DataStructure().top_directory_derivative[0],
                    region=region,
                    subdomain=subdomain,
                    experiment_type=experiment_type,
                    output_frequency=output_frequency,
                    grid_type=dict_json['output']['cefi_grid_type'],
                    release=dict_json['release']
                ).cefi_dir
                output_dir = os.path.join(local_top_dir, output_cefi_rel_path)

                # open the file
                with xr.open_dataset(file, chunks={}) as ds_var:
                    varname = dict_json['static_variable']

                    # create new filename based on original filename
                    new_filename = f'ocean_static.{varname}.nc'

                    # find if new file name already exist
                    new_file = os.path.join(output_dir, new_filename)
                    if os.path.exists(new_file):
                        logger_object.info(f"{new_file}: already exists. skipping...")
                    else:
                        # find the variable dimension info (for chunking)
                        logger_object.info(f"processing {new_file}")

                        # get xname and yname (need expand if there are other grids)
                        dims = list(ds_var[varname].dims)
                        if all(dim in dims for dim in ['xq', 'yh']):
                            xname = 'geolon_u'
                            yname = 'geolat_u'
                            xdimorder = dims.index('xq')
                            ydimorder = dims.index('yh')
                        elif all(dim in dims for dim in ['xh', 'yh']):
                            # currently only support tracer grid regridding
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
                            try:
                                raise ValueError("Unknown grid (need implementations)")
                            except ValueError as e:
                                logger_object.info(f"Skipping file due to error: {e}")
                                continue

                        # call regridding class
                        class_regrid = Regridding(ds_var, varname, xname, yname)
                        nx = len(ds_var[dims[xdimorder]])
                        ny = len(ds_var[dims[ydimorder]])
                        # perform regridding
                        ds_regrid = class_regrid.regrid_regular(nx, ny)

                        # copy the encoding and attributes
                        ds_regrid = mom6_encode_attr(ds_var, ds_regrid, var_names=[varname])

                        # redefine new global attribute
                        # global attributes
                        ds_regrid.attrs['cefi_variable'] = varname
                        ds_regrid.attrs['cefi_rel_path'] = output_dir
                        ds_regrid.attrs['cefi_filename'] = new_filename
                        ds_regrid.attrs['cefi_grid_type'] = dict_json['output']['cefi_grid_type']

                        # output the processed data
                        output_processed_data(
                            ds_regrid,
                            top_dir=dict_json['local_top_dir'],
                            dict_json_output=dict_json['output']
                        )
            except KeyError:
                # skip static file when regridding normal variables
                #  the error is due to missing static_variable in json file
                pass

if __name__=="__main__":

    client = Client(processes=False,memory_limit='500GB',silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 2:
        print("Usage: python mom6_regrid_batch.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]

    current_location = os.path.dirname(os.path.abspath(__file__))
    log_name = sys.argv[1].split('.')[0]+'.log'
    log_filename = os.path.join(current_location,log_name)

    # remove previous log file if exists
    if os.path.exists(log_filename):
        os.remove(log_filename)

    # Configure logging to write to both console and log file
    logging.basicConfig(
        level=logging.INFO,  # Log INFO and above
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),  # Log to file
            logging.StreamHandler()  # Log to console
        ]
    )
    logger = logging.getLogger()
    try:
       # Load the settings
        dict_json1 = load_json(json_setting,json_path=current_location)

        # preprocessing the file to cefi format
        regrid_batch(dict_json1,logger)

    except Exception as e:
        logger.exception("An exception occurred")

    finally:
        logger.info("Regridding process finished.")
