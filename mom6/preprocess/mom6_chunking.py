"""nco chunking
ncks -4 -L 5 --cnk_dmn time,100 --cnk_dmn lat,50 --cnk_dmn lon,50 input.nc output.nc

Increase the chunk info for hash search on the desired dimension and chunk size

"""

import os
import subprocess

def chunk_info()->dict:
    """dict of chunking info

    Returns
    -------
    dict
        dictionary of chunking info
    """
    dict_info = {
        'northwest_atlantic':{
            'hist_run':{
                'monthly':{
                    '3d':{
                        'tracer':{
                            'dimnames':['xh','yh','z_l','time'],
                            'chunks':[775,845,52,1]
                        }
                    },
                    '2d':{
                        'tracer':{
                            'dimnames':['xh','yh','time'],
                            'chunks':[206,225,22]
                        },
                        'ice':{
                            'dimnames':['xT','yT','time'],
                            'chunks':[206,225,22]
                        }
                    }
                },
                'daily':{
                    '2d':{
                        'tracer':{
                            'dimnames':['xh','yh','time'],
                            'chunks':[88,95,125]
                        },
                        'u':{
                            'dimnames':['xq','yh','time'],
                            'chunks':[88,95,125]
                        },
                        'v':{
                            'dimnames':['xh','yq','time'],
                            'chunks':[88,95,125]
                        }
                    }
                }
            }
        }
    }

    return dict_info

if __name__=="__main__":

    data_dir = os.getenv('DATASETSPRIVATE')

    # user input
    dir_path = f'{data_dir}regional_mom6/northwest_atlantic/hist_run/'
    filename = 'ocean_monthly_z.199301-201912.rhopot0.nc'
    REGION = 'northwest_atlantic'
    RUN = 'hist_run'
    FREQ = 'monthly'
    DIM = '3d'
    GRID = 'tracer'

    dict_chunk_info = chunk_info()[REGION][RUN][FREQ][DIM][GRID]
    dimnames = dict_chunk_info['dimnames']
    chunks = dict_chunk_info['chunks']


    # input file and output file names
    input_file = f'{dir_path}{filename}'
    output_file = f'{dir_path}{filename[:-3]}_chunked.nc'

    # NCO command for chunking
    nco_command = ['ncks', '-4', '-L', '3']
    for ndim,dim in enumerate(dimnames):
        nco_command += [
            '--cnk_dmn', f'{dim},{chunks[ndim]}'
        ]
    nco_command += [input_file, output_file]

    print(nco_command)

    # Run the NCO command using subprocess
    try:
        subprocess.run(nco_command, check=True)
        print(f'NCO command executed successfully. Output saved to {output_file}')
    except subprocess.CalledProcessError as e:
        print(f'Error executing NCO command: {e}')
