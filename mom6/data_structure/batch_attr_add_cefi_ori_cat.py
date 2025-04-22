"""
Add cefi_ori_category attrs to all nc files
"""


import os
import xarray as xr
from batch_change_attr import change_attr_ncatted

# utilizing the nco package ncatted command in change_attr_ncatted
# run c mode than run m mode to make sure all are updated

root_dirs = [
    '/Projects/CEFI/regional_mom6/cefi_portal/'
]
for root_dir in root_dirs:
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.nc') and 'static' not in filename:
                file_path = os.path.join(dirpath, filename)
                with xr.open_dataset(file_path,chunks={}) as ds:
                    cefi_category = ds.attrs['cefi_ori_filename'].split('.')[0]
                print(file_path) 
                print(cefi_category)
                change_attr_ncatted(
                    file_path,
                    attr_name='cefi_ori_category',
                    attr_value=cefi_category,
                    mode='m'
                )







