"""
This script is designed to do batch regridf of the 
regional mom6 output usig the new mom6_read module

The regridding is using the xesmf package with the bilinear method
there are other more conservative way to doing the regriddeding 
https://xesmf.readthedocs.io/en/stable/notebooks/Compare_algorithms.html 


"""
import os
import warnings
from pathlib import Path
import xarray as xr
from dask.distributed import Client
from prefect import flow, task, get_run_logger, serve
from mom6_rotate_batch import output_processed_data
from mom6.data_structure import portal_data
from mom6.mom6_module.mom6_read import AccessFiles
from mom6.mom6_module.mom6_regrid import Regridding
from mom6.mom6_module.mom6_export import mom6_encode_attr
from mom6.data_structure.batch_preprocess_hindcast import load_json
from mom6.data_structure.portal_data import DataStructure


@flow
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
    logger = get_run_logger()

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
        logger.info("Creating release folder in last level: %s", output_dir)
        # Create the directory
        os.makedirs(output_dir, exist_ok=True)
    else:
        logger.warning("release folder already exists: %s", output_dir)

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
            # Submit non-static regridding as an async task
            regrid_nonstatic(
                dict_json=dict_json,
                output_dir=output_dir,
                file=file,
                ds_static=ds_static,
                ds_static_ice=ds_static_ice
            )
        # regridding ocean static variables (one variable at a time)
        elif 'ocean_static.nc' in file:
            # Submit static regridding as an async task
            regrid_staticfield(
                dict_json=dict_json,
                output_dir=output_dir,
                file=file
            )
    
    return True  # or return any relevant data you need

@task(timeout_seconds=60*60*3, retries=3, retry_delay_seconds=5)
def regrid_nonstatic(
    dict_json:dict,
    output_dir:str,
    file:str,
    ds_static:xr.Dataset,
    ds_static_ice:xr.Dataset
):
    """regrid non static file

    Parameters
    ----------
    dict_json : dict
        json file in dictionary format
    output_dir : str
        output directory
    file : str
        processing file
    ds_static : xr.Dataset
        static field dataset
    ds_static_ice : xr.Dataset
        static ice field dataset

    Raises
    ------
    ValueError
        Unknow gridded data. Need implementation
    """

    logger = get_run_logger()
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
        logger.warning("%s: already exists. skipping...", new_file)
    else:
        # find the variable dimension info (for chunking)
        logger.info("processing %s", new_file)

        # get xname and yname (need expand if there are other grids)
        dims = list(ds_var.dims)
        if all(dim in dims for dim in ['xq', 'yh']):
            xname = 'geolon_u'
            yname = 'geolat_u'
            xdimorder = dims.index('xq')
            ydimorder = dims.index('yh')
            # stop regrid due to u grid need rotation first
            logger.warning("Skipping file due to UGRID need rotation first")
            return
        elif all(dim in dims for dim in ['xh', 'yh']):
            # currently only support tracer grid regridding
            xname = 'geolon'
            yname = 'geolat'
            xdimorder = dims.index('xh')
            ydimorder = dims.index('yh')
            # merge static field to include lon lat info
            ds_var = xr.merge([ds_var,ds_static],combine_attrs='override')
        elif all(dim in dims for dim in ['xh', 'yq']):
            xname = 'geolon_v'
            yname = 'geolat_v'
            xdimorder = dims.index('xh')
            ydimorder = dims.index('yq')
            # stop regrid due to v grid need rotation first
            logger.warning("Skipping file due to VGRID need rotation first")
            return
        elif all(dim in dims for dim in ['xT', 'yT']):
            # ice month static field replace
            # currently only support ice tracer grid regridding
            xname = 'GEOLON'
            yname = 'GEOLAT'
            xdimorder = dims.index('xT')
            ydimorder = dims.index('yT')
            # merge static field to include lon lat info
            ds_var = xr.merge([ds_var,ds_static_ice],combine_attrs='override')
        else:
            try:
                raise ValueError("Unknown grid (need implementations)")
            except ValueError as e:
                logger.exception("Skipping file due to error: %s", e)
                return

        # call regridding class
        class_regrid = Regridding(ds_var,varname,xname,yname)
        nx = len(ds_var[dims[xdimorder]])
        ny = len(ds_var[dims[ydimorder]])
        # perform regridding
        ds_regrid = class_regrid.regrid_regular(nx,ny)
        # forecast/reforecast files has two varname in one single file
        try:
            class_regrid_anom = Regridding(ds_var,varname+'_anom',xname,yname)
            # perform regridding
            ds_regrid_anom = class_regrid_anom.regrid_regular(nx,ny)
            ds_regrid = xr.merge([ds_regrid,ds_regrid_anom])
        except KeyError:
            pass

        # copy the encoding and attributes
        ds_regrid = mom6_encode_attr(ds_var,ds_regrid,var_names=[varname])

        # redefine new global attribute
        # global attributes
        ds_regrid.attrs['cefi_rel_path'] = output_dir
        ds_regrid.attrs['cefi_filename'] = new_filename
        ds_regrid.attrs['cefi_grid_type'] = dict_json['output']['cefi_grid_type']

        ds_regrid = ds_regrid.compute()

        # output the processed data
        output_processed_data(
            ds_regrid,
            top_dir=dict_json['local_top_dir'],
            dict_json_output=dict_json['output']
        )

@task(timeout_seconds=60*60*3, retries=3, retry_delay_seconds=5)
def regrid_staticfield(
    dict_json:dict,
    output_dir:str,
    file:str
):
    """regrid static field

    Parameters
    ----------
    dict_json : dict
        input json in dictionary format
    output_dir : str
        output directory
    file : str
        processing file

    Raises
    ------
    ValueError
        unknow static grid. Need implementation
    """
    logger = get_run_logger()
    try:
        # only regrid static field will have this variable in json file
        logger.info("output static field : %s", dict_json['static_variable'])

        # redefine output data path for static regrid
        local_top_dir=dict_json['local_top_dir']
        region=dict_json['region']
        subdomain=dict_json['subdomain']
        experiment_type=dict_json['experiment_type']
        output_frequency=dict_json['output_frequency']

        output_cefi_rel_path = portal_data.DataPath(
            top_directory=DataStructure().top_directory_derivative[0],
            region=region,
            subdomain=subdomain,
            experiment_type=experiment_type,
            output_frequency=output_frequency,
            grid_type=dict_json['output']['cefi_grid_type'],
            release=dict_json['release']
        ).cefi_dir
        output_dir = os.path.join(local_top_dir,output_cefi_rel_path)

        # open the file
        ds_var = xr.open_dataset(file,chunks={})
        varname = dict_json['static_variable']

        # create new filename based on original filename
        new_filename = f'ocean_static.{varname}.nc'

        # find if new file name already exist
        new_file = os.path.join(output_dir,new_filename)
        if os.path.exists(new_file):
            logger.warning("%s: already exists. skipping...", new_file)
        else:
            # find the variable dimension info (for chunking)
            logger.info("processing %s", new_file)

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
                    logger.exception("Skipping file due to error: %s", e)
                    return

            # call regridding class
            class_regrid = Regridding(ds_var,varname,xname,yname)
            nx = len(ds_var[dims[xdimorder]])
            ny = len(ds_var[dims[ydimorder]])
            # perform regridding
            ds_regrid = class_regrid.regrid_regular(nx,ny)

            # copy the encoding and attributes
            ds_regrid = mom6_encode_attr(ds_var,ds_regrid,var_names=[varname])

            # redefine new global attribute
            # global attributes
            ds_regrid.attrs['cefi_variable'] = varname
            ds_regrid.attrs['cefi_rel_path'] = output_dir
            ds_regrid.attrs['cefi_filename'] = new_filename
            ds_regrid.attrs['cefi_grid_type'] = dict_json['output']['cefi_grid_type']

            ds_regrid = ds_regrid.compute()
            # output the processed data
            output_processed_data(
                ds_regrid,
                top_dir=dict_json['local_top_dir'],
                dict_json_output=dict_json['output']
            )
            return

    except KeyError:
        # skip static file when regridding normal variables
        #  the error is due to missing static_variable in json file
        pass

@task
def json_parsing(jsonfile:str):
    """parse the json file to get the constant setting for regridding"""
    # Get the JSON file path from command-line arguments
    json_setting = jsonfile

    current_location = os.path.dirname(os.path.abspath(__file__))
    # log_name = sys.argv[1].split('.')[0]+'.log'
    # log_filename = os.path.join(current_location,log_name)

    # Load the settings
    dict_json = load_json(json_setting,json_path=current_location)

    return dict_json

@flow
def regrid_pipeline(jsonfile:str):
    """regridding pipeline"""
    logger = get_run_logger()

    try:
        # Submit json parsing as a task and get future
        dict_json_future = json_parsing(jsonfile)

        # Submit regrid batch with the future result
        regrid_batch(dict_json_future)

        return

    except Exception as e:
        logger.error("Pipeline failed: %s", str(e))
        raise

if __name__=="__main__":
    warnings.simplefilter("ignore")

    # Initialize dask client with proper cleanup
    try:
        client = Client(processes=False, memory_limit='500GB', silence_logs=50)
        print(client)
        print(client.cluster.dashboard_link)
        
        # regrid_pipeline()

        # Run the pipeline using serve single deployment
        # regrid_pipeline.serve(
        #     name="mom6-regrid-deployment-daily-serve",
        #     parameters={"jsonfile": 'mom6_regrid_batch_nwa_hcast_daily_raw.json'}
        # )

        # deploy multiple flow-runs
        nwa_hcast_daily = regrid_pipeline.to_deployment(
            name="mom6-regrid-deployment-daily-serve",
            parameters={"jsonfile": 'mom6_regrid_batch_nwa_hcast_daily_raw.json'}
        )
        nwa_hcast_mon = regrid_pipeline.to_deployment(
            name="mom6-regrid-deployment-serve",
            parameters={"jsonfile": 'mom6_regrid_batch_nwa_hcast_mon_raw.json'}
        )
        serve(nwa_hcast_daily, nwa_hcast_mon)

        # # Run the pipeline using deploy
        # regrid_pipeline.from_source(
        #     source=str(Path(__file__).parent),
        #     entrypoint="mom6_regrid_batch.py:regrid_pipeline",
        # ).deploy(
        #     name="mom6-regrid-deployment-daily",
        #     parameters={"jsonfile": 'mom6_regrid_batch_nwa_hcast_daily_raw.json'},
        #     work_pool_name="regional-mom6-processing",
        # )

        # regrid_pipeline.deploy(
        #     name="regrid-deployment",
        #     work_pool_name="regional-mom6-processing",
        #     # image="my-image",
        #     # push=False,
        #     # cron="* * * * *",
        # )

    finally:
        # Ensure client cleanup
        client.close()
