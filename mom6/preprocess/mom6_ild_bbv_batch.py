"""
This script is designed to do batch calculation of the 
isothermal layer depth and Brunt-Vaisala frequency

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
from mom6.mom6_module.mom6_ild import IsothermalLayerDepth
from mom6.mom6_module.mom6_bbv import BruntVaisalaFrequency
from mom6.mom6_module.mom6_export import mom6_encode_attr
from mom6.mom6_module.util import load_json, setup_logging, log_filename
from mom6.data_structure.portal_data import DataStructure

warnings.simplefilter("ignore")

def ild_bbv_batch(dict_json:dict):
    """perform the batch calculation of the isothermal layer depth
    and Brunt-Vaisala frequency

    Parameters
    ----------
    dict_json : dict
        dictionary that contain the constant setting in json

    """
    # get input data info
    local_top_dir=dict_json['local_top_dir']
    region=dict_json['region']
    subdomain=dict_json['subdomain']
    experiment_type=dict_json['experiment_type']
    output_frequency=dict_json['output_frequency']
    grid_type=dict_json['grid_type']
    release=dict_json['release']
    data_source=dict_json['data_source']

    # determine the data path for output data
    output_cefi_rel_path = portal_data.DataPath(
        top_directory=DataStructure().top_directory_derivative[0],
        region=region,
        subdomain=subdomain,
        experiment_type=experiment_type,
        output_frequency=output_frequency,
        grid_type=grid_type,
        release=release
    ).cefi_dir

    output_bbv_dir = os.path.join(local_top_dir,output_cefi_rel_path,'bbv')
    output_ild_dir = os.path.join(local_top_dir,output_cefi_rel_path,'ild')

    # Check if the ILD directory already exists in output data
    if not os.path.exists(output_ild_dir):
        logging.info("Creating ILD folder in last level: %s", output_ild_dir)
        # Create the directory
        os.makedirs(output_ild_dir, exist_ok=True)
    else:
        logging.info("ILD folder already exists: %s", output_ild_dir)

    # Check if the BBV directory already exists in output data
    if not os.path.exists(output_bbv_dir):
        logging.info("Creating BBV folder in last level: %s", output_bbv_dir)
        # Create the directory
        os.makedirs(output_bbv_dir, exist_ok=True)
    else:
        logging.info("BBV folder already exists: %s", output_bbv_dir)

    # get input data
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

    tos_path = local_access.get(variable='tos')[0]
    thetao_path = local_access.get(variable='thetao')[0]
    so_path = local_access.get(variable='so')[0]

    if grid_type == 'raw':
        statics_path = local_access.get(variable='ocean_static')[0]
        # prepare static data and lazy load it
        try:
            # time dim not needed appeared in the first version of NWA data
            ds_static = xr.open_dataset(statics_path,chunks={'yh': 50, 'xh': 50}).drop_vars('time')
        except ValueError:
            ds_static = xr.open_dataset(statics_path)
        da_lon = ds_static['geolon']
        da_lat = ds_static['geolat']
        da_bottom = ds_static['deptho']

        # prepare dataset and lazy load them
        ds_so = xr.open_dataset(
            so_path,
            chunks={'time': 1, 'yh': 50, 'xh': 50, 'z_l': -1}
        )
        ds_thetao = xr.open_dataset(
            thetao_path,
            chunks={'time': 1, 'yh': 50, 'xh': 50, 'z_l': -1}
        )
        ds_tos = xr.open_dataset(
            tos_path,
            chunks={'time': 1, 'yh': 50, 'xh': 50}
        )

        da_z = ds_thetao['z_l']

    elif grid_type == 'regrid':

        derivative_path = portal_data.DataPath(
            top_directory=DataStructure().top_directory_derivative[0],
            region=region,
            subdomain=subdomain,
            experiment_type=experiment_type,
            output_frequency=output_frequency,
            grid_type=grid_type,
            release=release
        ).cefi_dir

        statics_path = os.path.join(
            local_top_dir,
            derivative_path,
            'static',
            'ocean_static.deptho.nc'
        )
        # prepare static data and lazy load it
        ds_static = xr.open_dataset(statics_path, chunks={'lat': 50, 'lon': 50})
        da_bottom = ds_static['deptho']

        # prepare dataset and lazy load them
        ds_so = xr.open_dataset(
            so_path,
            chunks={'time': 1, 'lat': 50, 'lon': 50, 'z_l': -1}
        )
        ds_thetao = xr.open_dataset(
            thetao_path,
            chunks={'time': 1, 'lat': 50, 'lon': 50, 'z_l': -1}
        )
        ds_tos = xr.open_dataset(
            tos_path,
            chunks={'time': 1, 'lat': 50, 'lon': 50}
        )
        da_z = ds_thetao['z_l']
        da_lon = ds_thetao['lon']
        da_lat = ds_thetao['lat']
    else:
        raise ValueError(f"Unsupported grid type: {grid_type}")

    # calculate the BBV
    # more worker less threads due to GIL of the task involved
    ild_obj = IsothermalLayerDepth(
        da_sst=ds_tos['tos'],
        da_thetao=ds_thetao['thetao'],
        da_depth=da_z,
        da_bottom_depth=da_bottom,
        ild_temp_offset=0.5,
        depth_dim_name='z_l'
    )
    ds_ild = ild_obj.calculate_ild()

    # copy the encoding and attributes
    varname = 'ild'
    ds_ild = mom6_encode_attr(ds_thetao, ds_ild, var_names=[varname])

    # create new filename based on original filename
    filename = ds_thetao.attrs['cefi_filename']
    filename_seg = filename.split('.')
    filename_seg[0] = varname
    new_filename = '.'.join(filename_seg)

    # defined filename and path used for output
    ds_ild.attrs['cefi_rel_path'] = os.path.join(output_cefi_rel_path,varname)
    ds_ild.attrs['cefi_filename'] = new_filename
    ds_ild.attrs['cefi_variable'] = varname
    ds_ild.attrs['cefi_ori_filename'] = 'N/A'
    ds_ild.attrs['cefi_ori_category'] = 'N/A'
    ds_ild.attrs['cefi_aux'] = "Postprocessed Data : derived Isothermal Layer Depth"

    # find if new file name already exist
    new_file = os.path.join(output_ild_dir, new_filename)
    if os.path.exists(new_file):
        logging.info("%s: already exists. skipping...", new_file)
    else:

        # find the variable dimension info (for chunking)
        logging.info("Computing...ILD...")
        
        # perform calculation
        ds_ild = ds_ild.compute()

        # find the variable dimension info (for chunking)
        logging.info("Outputing %s", new_file)

        # output the processed data
        output_processed_data(
            ds_ild,
            top_dir=dict_json['local_top_dir']
        )

        # close dataset
        ds_tos.close()
        ds_ild.close()
        logging.info("ILD and SST closed")

    # calculate the BBV
    # more worker less threads due to GIL of the task involved
    bbv_obj = BruntVaisalaFrequency(
        da_thetao=ds_thetao['thetao'],
        da_so=ds_so['so'],
        da_depth=da_z,
        da_lon=da_lon,
        da_lat=da_lat,
        eos_version='teos-10',
        interp_method='cubic',
        depth_dim_name='z_l'
    )
    ds_bbv = bbv_obj.calculate_bbv()

    # copy the encoding and attributes
    varname = 'bbv'
    ds_bbv = mom6_encode_attr(ds_thetao, ds_bbv, var_names=[varname])

    # create new filename based on original filename
    filename = ds_thetao.attrs['cefi_filename']
    filename_seg = filename.split('.')
    filename_seg[0] = varname
    new_filename = '.'.join(filename_seg)

    # defined filename and path used for output
    ds_bbv.attrs['cefi_rel_path'] = os.path.join(output_cefi_rel_path,varname)
    ds_bbv.attrs['cefi_filename'] = new_filename
    ds_bbv.attrs['cefi_variable'] = varname
    ds_bbv.attrs['cefi_ori_filename'] = 'N/A'
    ds_bbv.attrs['cefi_ori_category'] = 'N/A'
    ds_bbv.attrs['cefi_aux'] = "Postprocessed Data : derived Brunt-Vaisala Frequency"

    # find if new file name already exist
    new_file = os.path.join(output_bbv_dir, new_filename)
    if os.path.exists(new_file):
        logging.info("%s: already exists. skipping...", new_file)
    else:
        # find the variable dimension info (for chunking)
        logging.info("Outputing %s", new_file)

        # perform calculation
        ds_bbv = ds_bbv.compute()

        # close datasets
        ds_thetao.close()
        ds_so.close()

        # output the processed data
        output_processed_data(
            ds_bbv,
            top_dir=dict_json['local_top_dir']
        )
        ds_bbv.close()


if __name__=="__main__":


    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 2:
        print("Usage: python mom6_ild_bbv_batch.py xxxx.json")
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

        client = Client(n_workers=dict_json1['worker_number'], threads_per_worker=1)
        print(client.cluster.dashboard_link)

        ild_bbv_batch(dict_json1)

    except Exception as e:
        logging.exception("An exception occurred")

    finally:
        logging.info("ILD BBV calculation finished.")
