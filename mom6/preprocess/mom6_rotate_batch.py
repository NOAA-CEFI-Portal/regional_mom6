"""
This script is designed to do batch rotate of the 
regional mom6 output usig the new mom6_read module

The regridding is using the xesmf package with the bilinear method
there are other more conservative way to doing the regriddeding 
https://xesmf.readthedocs.io/en/stable/notebooks/Compare_algorithms.html 


"""
import os
import sys
import time
import logging
import warnings
from typing import Optional
import xarray as xr
from dask.distributed import Client
from mom6.mom6_module.mom6_read import AccessFiles
from mom6.mom6_module.mom6_vector_rotate import VectorRotation
from mom6.mom6_module.util import load_json, setup_logging, log_filename
from mom6.data_structure import portal_data

warnings.simplefilter("ignore")


def output_processed_data(
    ds:xr.Dataset,
    top_dir:str,
    dict_json_output:Optional[dict]=None,
    max_attempts:int=10
):
    """output the processed data to the netcdf file

    Parameters
    ----------
    ds : xr.dataset
        dataset that contain the processed data
    dict_json_output : dict
        dictionary that contain the output meta data
    top_dir : str
        top directory of the output file
    """

    if dict_json_output is None:
        print('No cefi attributes provided')
    else:
        for key in dict_json_output:
            ds.attrs[key] = dict_json_output[key]
            print(f"modified {key}: {dict_json_output[key]}")

    abs_path = os.path.join(top_dir,ds.attrs['cefi_rel_path'])
    output_file = os.path.join(abs_path,ds.attrs['cefi_filename'])

    # assign chunk size for different dim (same order as the dims)
    #  chunk size design in portal_data.py
    varnames = list(ds.variables)
    dims = list(ds.dims)
    coords =list(ds.coords)
    variables = []
    for var in varnames:
        if var not in dims and var not in coords:
            variables.append(var)

    chunks = []
    chunk_info = portal_data.FileChunking()
    for dim in dims:
        if isinstance(dim, str):
            if 'z' in dim :
                chunks.append(chunk_info.vertical)
            elif 'time' in dim:
                chunks.append(chunk_info.time)
            elif 'lead' in dim:
                chunks.append(chunk_info.lead)
            elif 'member' in dim:
                chunks.append(chunk_info.member)
            elif 'init' in dim:
                chunks.append(chunk_info.init)
            else:
                chunks.append(chunk_info.horizontal)

    for var in variables:
        if len(ds[var].dims) == len(chunks):
            # variable dimensions does not have all dimension of the dataset (tercile prob)
            # => no chunking
            #  TODO: need to refactor in the future
            ds[var].encoding = {
                'zlib': True,
                'szip': False,
                'zstd': False,
                'bzip2': False,
                'blosc': False,
                'shuffle': True,
                'complevel': 2,
                'fletcher32': False,
                'contiguous': False,
                'chunksizes': chunks
            }

    ds = ds.compute()

    attempt = 0
    print(f"Outputing file: {output_file}")
    while attempt < max_attempts:
        try:
            print(f"Attempt {attempt + 1}/{max_attempts} to write {output_file}")
            ds.to_netcdf(output_file)
            break
        except (RuntimeError, PermissionError) as e:
            print(f"Error during file write (attempt {attempt + 1}): {e}")
            if "NetCDF: HDF error" in str(e) or "Permission denied" in str(e):
                if attempt < max_attempts - 1:
                    delay = 1. * (2. ** attempt) # 2 second for initial retry, 4s next
                    print(f"Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                    attempt += 1
                else:
                    print("Max retry attempts reached for netcdf write. Skipping for failing.")
                    logging.warning("%s not saved. An NetCDF: HDF error or Permission denied error occurred: %s.", output_file,e)
                    break
            else:
                logging.warning("%s not saved. An unexpected error occurred: %s.", output_file,e)
        except Exception as e:
            # this print the error message but does not stop the file processing
            logging.warning("%s not saved. An unexpected error occurred: %s.", output_file,e)


def rotate_batch(dict_json:dict)->tuple:
    """perform the batch rotation of the mom6 output

    Parameters
    ----------
    dict_json : dict
        dictionary that contain the constant setting in json

    Returns
    -------
    tuple
        a tuple of two xr.Dataset, the first is the rotated u,
        the second is the rotated v
    """

    local_top_dir=dict_json['local_top_dir']
    region=dict_json['region']
    subdomain=dict_json['subdomain']
    experiment_type=dict_json['experiment_type']
    output_frequency=dict_json['output_frequency']
    grid_type=dict_json['grid_type']
    release=dict_json['release']
    data_source=dict_json['data_source']
    u_name = dict_json['u_name']
    v_name = dict_json['v_name']

    new_file_u = os.path.join(
        local_top_dir,
        dict_json['output_u']['cefi_rel_path'],
        dict_json['output_u']['cefi_filename']
    )
    new_file_v = os.path.join(
        local_top_dir,
        dict_json['output_v']['cefi_rel_path'],
        dict_json['output_v']['cefi_filename']
    )

    if os.path.exists(new_file_u) and os.path.exists(new_file_v):
        logging.info("%s: already exists. skipping...", new_file_u)
        logging.info("%s: already exists. skipping...", new_file_v)
        logging.info("rotation complete cleanly")
        sys.exit(0)
    else:
        # find the variable dimension info (for chunking)
        logging.info(
            "At least one of the rotated vector is not produced \n"
            "processing %s and %s",
            new_file_u,
            new_file_v
        )

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

    try:
        ufile_list = local_access.get(variable=u_name)
        vfile_list = local_access.get(variable=v_name)
    except FileNotFoundError as e:
        logging.error(
            "FileNotFoundError: %s\n"
            "Please check the input file path",
            e
        )
        sys.exit(1)
    statics = local_access.get(variable='ocean_static')
    rotations = local_access.get(variable='ice_static')

    ds_u = xr.open_mfdataset(
        ufile_list,
        combine='by_coords',
        parallel=True
    )

    ds_v = xr.open_mfdataset(
        vfile_list,
        combine='by_coords',
        parallel=True
    )

    # prepare static data
    try:
        ds_static = xr.open_dataset(statics[0]).drop_vars('time') # time dim not needed
    except ValueError:
        ds_static = xr.open_dataset(statics[0])

    # prepare the rotation matrix to regular coord names

    ds_rotate = xr.open_dataset(rotations[0])
    ds_rotate = (ds_rotate
        .rename({
            'yT':'yh',
            'xT':'xh',
            'GEOLON':'geolon',
            'GEOLAT':'geolat',
            'COSROT':'cosrot',
            'SINROT':'sinrot'
        })
        .set_coords(
            ['geolon','geolat']
        )
    )


    # merge static field to include lon lat info
    ds_u = xr.merge([ds_u,ds_static],combine_attrs='override')
    ds_v = xr.merge([ds_v,ds_static],combine_attrs='override')

    # setup the rotation class
    class_rotate = VectorRotation(ds_u,u_name,ds_v,v_name,ds_rotate)

    # perform regrid => rotate => return compute
    dict_uv = class_rotate.generate_true_uv()

    ds_u_true = xr.Dataset()
    ds_v_true = xr.Dataset()
    ds_u_true[f"{dict_json['output_u']['cefi_variable']}"] = dict_uv['u']
    ds_v_true[f"{dict_json['output_v']['cefi_variable']}"] = dict_uv['v']

    # prepare attributes for variables
    # global attributes
    ds_u_true.attrs = ds_u.attrs
    ds_v_true.attrs = ds_v.attrs

    # attributes for each dimension
    for ds_vector in [ds_u_true,ds_v_true]:
        dims = list(ds_vector.dims)
        for dim in dims:
            try:
                ds_vector[dim].attrs = ds_static[dim].attrs
                ds_vector[dim].encoding = ds_static[dim].encoding
            except KeyError:
                pass

    # attributes for time
    ds_u_true['time'].attrs = ds_u['time'].attrs
    ds_v_true['time'].attrs = ds_v['time'].attrs

    # attributes for each variables
    ds_u_true[f"{dict_json['output_u']['cefi_variable']}"].attrs = ds_u[u_name].attrs
    ds_v_true[f"{dict_json['output_v']['cefi_variable']}"].attrs = ds_v[v_name].attrs
    ds_u_true[f"{dict_json['output_u']['cefi_variable']}"].attrs['long_name'] = (
        f"Rotated {ds_u[u_name].attrs['long_name']}"
    )
    ds_v_true[f"{dict_json['output_v']['cefi_variable']}"].attrs['long_name'] = (
        f"Rotated {ds_v[v_name].attrs['long_name']}"
    )

    return ds_u_true,ds_v_true


if __name__=="__main__":

    client = Client(processes=False,memory_limit='500GB',silence_logs=50)
    # print(client)
    # print(client.cluster.dashboard_link)

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 1:
        print("Usage: python mom6_rotate_batch.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]
    logfilename = log_filename(json_setting)
    current_location = os.path.dirname(os.path.abspath(__file__))
    logfilename = os.path.join(current_location,logfilename)

    # remove previous log file if exists
    if os.path.exists(logfilename):
        os.remove(logfilename)

    setup_logging(logfilename)

    try:
        # Load the settings
        dict_json1 = load_json(json_setting,json_path=current_location)

        # preprocessing the file to cefi format
        ds_u_x,ds_v_y = rotate_batch(dict_json1)

        ds_u_x = ds_u_x.compute()
        ds_v_y = ds_v_y.compute()

        # output the processed data
        try:
            output_processed_data(
                ds_u_x,
                top_dir=dict_json1['local_top_dir'],
                dict_json_output=dict_json1['output_u']
            )
            output_processed_data(
                ds_v_y,
                top_dir=dict_json1['local_top_dir'],
                dict_json_output=dict_json1['output_v']
            )
        except PermissionError as e:
            logging.error(
                "PermissionError: %s\n"
                "Please check the output directory permission",
                e
            )
            sys.exit(1)

        logging.info("rotation complete cleanly")
    except Exception as e:
        logging.exception("An exception occurred")
    finally:
        logging.info("Rotate process finished.")
