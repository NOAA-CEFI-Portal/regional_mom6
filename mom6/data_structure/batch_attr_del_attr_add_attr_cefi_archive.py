"""
fix cefi_rel_path that miss record abs path not relative path


DEBUGGING - still not working
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
                
                with xr.open_dataset(file_path, chunks={}) as ds:
                    for attr in ds.attrs:
                        if 'cefi_archive_version_ens' in attr:
                            attr_value = ds.attrs[attr]
                            ens_num = attr.split('_')[-1]

                            # print(file_path)
                            # print(attr)
                            # print(ens_num)
                            # print(attr_value)

                            # create new attr GFDL archive version
                            change_attr_ncatted(
                                file_path,
                                attr_name=f'gfdl_archive_version_{ens_num}',
                                attr_value=attr_value,
                                mode='c'
                            )

                            # remove new attr GFDL archive version
                            change_attr_ncatted(
                                file_path,
                                attr_name=attr,
                                attr_value=None,
                                mode='d'
                            )
