"""
fix cefi_rel_path that miss record abs path not relative path
"""

import os
import xarray as xr
from batch_change_attr import change_attr_ncatted

root_dirs = [
    '/Projects/CEFI/regional_mom6/cefi_portal/'
]
for root_dir in root_dirs:
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.nc') and 'static' not in filename:
                file_path = os.path.join(dirpath, filename)
                with xr.open_dataset(file_path,chunks={}) as ds:
                    path_seg = ds.attrs['cefi_rel_path'].split('/')
                    first_dir = path_seg[0]
                print(file_path)
                print(first_dir)
                if first_dir != 'cefi_portal':
                    cefi_rel_path = '/'.join(path_seg[4:])
                    print(cefi_rel_path)
                    change_attr_ncatted(
                        file_path,
                        attr_name='cefi_rel_path',
                        attr_value=cefi_rel_path,
                        mode='m'
                    )







