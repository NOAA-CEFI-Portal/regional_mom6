
import xarray as xr
import numpy as np
import os


dir = '/Projects/CEFI/private/scratch/chsu/NWA_reforecast_r20240213'
filename_header = 'seasonal.ocean_month'


start_year = 2007
end_year = 2007
ini_mon = 3

output_dir = os.path.join(dir, 'concat')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for year in range(start_year, end_year + 1):
    output_file = os.path.join(output_dir, f'tos.{filename_header}.i{year}{ini_mon:02d}.nc')

    if os.path.exists(output_file):
        print(f'File already exists skipping: {output_file}')
        continue

    concat_enss = []
    for ens in range(1, 10+1):
        file = os.path.join(dir, f'{filename_header}.i{year}{ini_mon:02d}.e{ens:02d}.tos.nc')

        if os.path.exists(file):
            print(f'Processing {file}')
            with xr.open_dataset(file) as ds:
                ds = ds.rename({'time': 'lead'})
                ds = ds.sortby('lead')
                concat_enss.append(ds)
        else:
            print(f'File not found: {file}')

    ds_concat = xr.concat(concat_enss, dim='member')

    ds_concat['tos'] = ds_concat['tos'].transpose('lead', 'member', 'yh', 'xh')

    ds_concat['init'] = np.datetime64(f'{year}-{ini_mon:02d}-01').astype('datetime64[ns]')

    ds_concat['member'] = np.arange(1, 10+1)

    ds_concat = ds_concat.drop_dims('time', errors='ignore')
    ds_concat = ds_concat.drop_dims('nv', errors='ignore')

    print(f'Saving concatenated file: {output_file}')
    ds_concat.to_netcdf(os.path.join(dir,'concat', output_file), unlimited_dims=[])

