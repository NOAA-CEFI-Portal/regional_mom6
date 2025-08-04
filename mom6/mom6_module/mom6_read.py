#!/usr/bin/env python
"""
The module is created to read CEFI 
regional mom6 data under the CEFI 
data structure.
"""
import os
import glob
import warnings
from typing import Optional
import requests
import fsspec
from bs4 import BeautifulSoup, NavigableString
import xarray as xr
from mom6.data_structure import portal_data
from mom6.mom6_module.mom6_types import (
    ModelRegionOptions,
    ModelSubdomainOptions,
    ModelExperimentTypeOptions,
    ModelOutputFrequencyOptions,
    ModelGridTypeOptions,
    DataSourceOptions
)

warnings.simplefilter("ignore")
xr.set_options(keep_attrs=True)

class OpenDapStore:
    """class to handle the OPeNDAP request
    """
    def __init__(
        self,
        region : ModelRegionOptions,
        subdomain : ModelSubdomainOptions,
        experiment_type : ModelExperimentTypeOptions,
        output_frequency : ModelOutputFrequencyOptions,
        grid_type : ModelGridTypeOptions,
        release : str
    ) -> None:
        """input for the class to get the opendap path

        Parameters
        ----------
        region : ModelRegionOptions
            region name
        subdomain : ModelSubdomainOptions
            subdomain name
        experiment_type : ModelExperimentTypeOptions
            experiment type
        output_frequency : ModelOutputFrequencyOptions
            data output frequency
        grid_type : ModelGridTypeOptions
            model grid type
        release_date : str
            release date in the format of "rYYYYMMDD"
        """
        self.region = region
        self.subdomain = subdomain
        self.experiment_type = experiment_type
        self.output_frequency = output_frequency
        self.grid_type = grid_type
        self.release = release

        # check kwarg input exists using data class
        cefi_data_path = portal_data.DataPath(
            region=region,
            subdomain=subdomain,
            experiment_type=experiment_type,
            output_frequency=output_frequency,
            grid_type=grid_type,
            release=release
        )

        self.cefi_rel_dir = cefi_data_path.cefi_dir

        # construct the catalog/opendap url
        CATALOG_HEAD = 'https://psl.noaa.gov/thredds/catalog/'
        OPENDAP_HEAD = 'https://psl.noaa.gov/thredds/dodsC/'
        REGIONAL_MOM6_PATH = 'Projects/CEFI/regional_mom6'

        self.catalog_url = os.path.join(
            CATALOG_HEAD,
            REGIONAL_MOM6_PATH,
            self.cefi_rel_dir,
            'catalog.html'
        )

        self.opendap_url = os.path.join(
            OPENDAP_HEAD,
            REGIONAL_MOM6_PATH,
            self.cefi_rel_dir
        )

        try:
            # Make the request
            html_response = requests.get(self.catalog_url, timeout=10)

            if html_response.status_code == 200:
                # Response is OK
                print(f"Success: URL {self.catalog_url} responded with status 200.")

            else:
                # dealing with the non-200 response due to the release date
                parent_dir_release = os.path.dirname(self.cefi_rel_dir)
                parent_dir_release_catalog_url = os.path.join(
                    CATALOG_HEAD,
                    REGIONAL_MOM6_PATH,
                    parent_dir_release,
                    'catalog.html'
                )
                release_response = requests.get(parent_dir_release_catalog_url, timeout=10)
                # find the parent before release exist
                if release_response.status_code == 200:
                    # Parse the html response
                    soup = BeautifulSoup(release_response.text, 'html.parser')
                    # get all div tag with class name including "content"
                    div_content = soup.find('div', class_='content')
                    if div_content and not isinstance(div_content, NavigableString):
                        # get all a tag within the subset div_content
                        a_tags = div_content.find_all('a')
                        # get all code tag within the subset a_tags
                        all_release_list = [a_tag.find_all('code')[0].text for a_tag in a_tags]
                        print('--------------------------------')
                        print('Current release data is not valid. Available releases are:')
                        for release_dir in all_release_list:
                            print(release_dir)
                        print('--------------------------------')
                        raise FileNotFoundError('No files available based on release date, check available release data above')
                # this else should not be reach when THREDD server is working.
                # Error should be catched when constructing cefi_data_path.
                # This error is reach when the connection is not available 
                else:
                    # Non-200 response
                    print(
                        f"URL {self.catalog_url} "+
                        f"responded with status {html_response.status_code}."
                    )
                    # Perform additional error handling here
                    raise ConnectionError(f'Error: Connection status code {html_response.status_code}')
        except requests.exceptions.RequestException as e:
            # Log the exception
            print(f"Failed to connect to {self.catalog_url}. Exception: {e}")
            # Handle connection failure here
            raise ConnectionError('Error: Server not responding.') from e

    def get_files(self,variable:Optional[str]=None)-> list:
        """Getting file opendap urls

        Parameters
        ----------
        variable : str
            variable short name ex:'tos' for sea surface temperature
        
        Returns
        -------
        list
            a list of url in the form of string that 
            provide the locations of the data when
            accessing using opendap

        Raises
        ------
        FileNotFoundError
            When the files is empty that means the init setting 
            or code must have some incorrect pairing. Debug possibly 
            needed.
        """

        html_response = requests.get(self.catalog_url, timeout=10)

        # Parse the html response
        soup = BeautifulSoup(html_response.text, 'html.parser')

        # get all div tag with class name including "content"
        div_content = soup.find('div', class_='content')
        if div_content and not isinstance(div_content, NavigableString):
            # get all a tag within the subset div_content
            a_tags = div_content.find_all('a')
            # get all code tag within the subset a_tags
            all_file_list = [a_tag.find_all('code')[0].text for a_tag in a_tags]

            # include only netcdf file
            files = []
            if variable is None:
                for file in all_file_list:
                    if 'nc' == file.split('.')[-1] :
                        files.append(
                            os.path.join(self.opendap_url,file)
                        )
            else:
                for file in all_file_list:
                    if 'nc' == file.split('.')[-1] and variable == file.split('.')[0] :
                        files.append(
                            os.path.join(self.opendap_url,file)
                        )

            # if zero file is found
            if not files :
                raise FileNotFoundError('No files available based on input')

            return files
        else:
            raise FileNotFoundError('No div_content for BeautifulSoup based on input')
            


class GCSStore:
    """class to handle the Google Cloud Storage request
    """
    def __init__(
        self,
        region : ModelRegionOptions,
        subdomain : ModelSubdomainOptions,
        experiment_type : ModelExperimentTypeOptions,
        output_frequency : ModelOutputFrequencyOptions,
        grid_type : ModelGridTypeOptions,
        release : str
    ) -> None:
        """input for the class to get the GCS object full name

        Parameters
        ----------
        region : ModelRegionOptions
            region name
        subdomain : ModelSubdomainOptions
            subdomain name
        experiment_type : ModelExperimentTypeOptions
            experiment type
        output_frequency : ModelOutputFrequencyOptions
            data output frequency
        grid_type : ModelGridTypeOptions
            model grid type
        release_date : str
            release date in the format of "rYYYYMMDD"
        """
        self.region = region
        self.subdomain = subdomain
        self.experiment_type = experiment_type
        self.output_frequency = output_frequency
        self.grid_type = grid_type
        self.release = release

        # check kwarg input exists using data class
        cefi_data_path = portal_data.DataPath(
            region=region,
            subdomain=subdomain,
            experiment_type=experiment_type,
            output_frequency=output_frequency,
            grid_type=grid_type,
            release=release
        )

        # remove "cefi_portal" dir at the top rel path
        parse_path = cefi_data_path.cefi_dir
        self.cefi_rel_dir = os.path.join(*parse_path.split('/')[1:])


        # construct the catalog/opendap url
        self.cloud_head = 'gcs://'
        self.cloud_obj_proj_name = 'gcs://noaa-oar-cefi-regional-mom6/'

        self.cloud_obj_parent_name = os.path.join(
            self.cloud_obj_proj_name,
            self.cefi_rel_dir
        )
        # print(self.cefi_rel_dir)

        try:
            # Make the request
            fs = fsspec.filesystem("gcs", anon=True)
            self.all_objs = fs.ls(self.cloud_obj_parent_name, detail=False)
            print(f"Files are available on GCS ({self.cloud_obj_parent_name})")
        except FileNotFoundError as e:
            gcs_url = 'https://console.cloud.google.com/storage/browser/noaa-oar-cefi-regional-mom6'
            raise FileNotFoundError(f"Please check if the release number is available at {gcs_url}.") from e


    def get_files(self,variable:Optional[str]=None)-> list:
        """Getting file GCS links

        Parameters
        ----------
        variable : str
            variable short name ex:'tos' for sea surface temperature
        
        Returns
        -------
        list
            a list of url in the form of string that 
            provide the locations of the data when
            accessing using GCS

        Raises
        ------
        FileNotFoundError
            When the files is empty that means the init setting 
            or code must have some incorrect pairing. Debug possibly 
            needed.
        """


        # include only json index file and netcdf file
        files = []
        ncfiles = []
        if variable is None:
            for file in self.all_objs:
                if 'json' == file.split('.')[-1] :
                    cloud_store = self.cloud_head+file
                    files.append(cloud_store)
                elif 'nc' == file.split('.')[-1] :
                    cloud_store = self.cloud_head+file
                    ncfiles.append(cloud_store)
        else:
            for file in self.all_objs:
                var_filename = file.split('/')[-1]
                if 'json' == var_filename.split('.')[-1] and variable == var_filename.split('.')[0] :
                    cloud_store = self.cloud_head+file
                    files.append(cloud_store)
                elif 'nc' == var_filename.split('.')[-1] and variable == var_filename.split('.')[0]:
                    cloud_store = self.cloud_head+file
                    ncfiles.append(cloud_store)

        # if zero file is found
        if not files and not ncfiles:
            raise FileNotFoundError('No files available based on input')
        elif not files and len(ncfiles)>0:
            raise FileNotFoundError('Kerchunk json file not ready')

        return files



class S3Store:
    """class to handle the AWS S3 bucket request
    """
    def __init__(
        self,
        region : ModelRegionOptions,
        subdomain : ModelSubdomainOptions,
        experiment_type : ModelExperimentTypeOptions,
        output_frequency : ModelOutputFrequencyOptions,
        grid_type : ModelGridTypeOptions,
        release : str
    ) -> None:
        """input for the class to get the S3 object full name

        Parameters
        ----------
        region : ModelRegionOptions
            region name
        subdomain : ModelSubdomainOptions
            subdomain name
        experiment_type : ModelExperimentTypeOptions
            experiment type
        output_frequency : ModelOutputFrequencyOptions
            data output frequency
        grid_type : ModelGridTypeOptions
            model grid type
        release_date : str
            release date in the format of "rYYYYMMDD"
        """
        self.region = region
        self.subdomain = subdomain
        self.experiment_type = experiment_type
        self.output_frequency = output_frequency
        self.grid_type = grid_type
        self.release = release

        # check kwarg input exists using data class
        cefi_data_path = portal_data.DataPath(
            region=region,
            subdomain=subdomain,
            experiment_type=experiment_type,
            output_frequency=output_frequency,
            grid_type=grid_type,
            release=release
        )

        # remove "cefi_portal" dir at the top rel path
        parse_path = cefi_data_path.cefi_dir
        self.cefi_rel_dir = os.path.join(*parse_path.split('/')[1:])


        # construct the catalog/opendap url
        self.cloud_head = 's3://'
        self.cloud_obj_proj_name = 's3://noaa-oar-cefi-regional-mom6-pds/'

        self.cloud_obj_parent_name = os.path.join(
            self.cloud_obj_proj_name,
            self.cefi_rel_dir
        )
        # print(self.cefi_rel_dir)

        try:
            # Make the request
            fs = fsspec.filesystem("s3", anon=True)
            self.all_objs = fs.ls(self.cloud_obj_parent_name, detail=False)
            print(f"Files are available on S3 ({self.cloud_obj_parent_name})")
        except FileNotFoundError :
            s3_url = 'https://noaa-oar-cefi-regional-mom6-pds.s3.amazonaws.com/index.html'
            print(f"Please check if the release number is available at {s3_url}.")


    def get_files(self,variable:Optional[str]=None)-> list:
        """Getting file S3 links

        Parameters
        ----------
        variable : str
            variable short name ex:'tos' for sea surface temperature
        
        Returns
        -------
        list
            a list of links in the form of string that 
            provide the locations of the data when
            accessing using S3

        Raises
        ------
        FileNotFoundError
            When the files is empty that means the init setting 
            or code must have some incorrect pairing. Debug possibly 
            needed.
        """


        # include only json index file and netcdf file
        files = []
        ncfiles = []
        if variable is None:
            for file in self.all_objs:
                if 'json' == file.split('.')[-1] :
                    cloud_store = self.cloud_head+file
                    files.append(cloud_store)
                elif 'nc' == file.split('.')[-1] :
                    cloud_store = self.cloud_head+file
                    ncfiles.append(cloud_store)
        else:
            for file in self.all_objs:
                var_filename = file.split('/')[-1]
                if 'json' == var_filename.split('.')[-1] and variable == var_filename.split('.')[0] :
                    cloud_store = self.cloud_head+file
                    files.append(cloud_store)
                elif 'nc' == var_filename.split('.')[-1] and variable == var_filename.split('.')[0]:
                    cloud_store = self.cloud_head+file
                    ncfiles.append(cloud_store)

        # if zero file is found
        if not files and not ncfiles:
            raise FileNotFoundError('No files available based on input')
        elif not files and len(ncfiles)>0:
            raise FileNotFoundError('Kerchunk json file not ready')

        return files


class LocalStore:
    """class to handle the local read request
    """
    def __init__(
        self,
        local_top_dir : str,
        region : ModelRegionOptions,
        subdomain : ModelSubdomainOptions,
        experiment_type : ModelExperimentTypeOptions,
        output_frequency : ModelOutputFrequencyOptions,
        grid_type : ModelGridTypeOptions,
        release : str
    ) -> None:
        """getting local directory structure under CEFI structure
        the existence of the CEFI structure is needed to be able to 
        use this class

        Parameters
        ----------
        local_top_dir : str
            the absolution path to the local CEFI data.
            should be the absolute path before cefi_portal/...
        region : ModelRegionOptions
            region name
        subdomain : ModelSubdomainOptions
            subdomain name
        experiment_type : ModelExperimentTypeOptions
            experiment type
        output_frequency : ModelOutputFrequencyOptions
            data output frequency
        grid_type : ModelGridTypeOptions
            model grid type
        release_date : str
            release date in the format of "rYYYYMMDD"

        Raises
        ------
        FileNotFoundError
            When there is no data structure or 
            data structure constructed by input does not exist
        """
        self.local_top_dir = local_top_dir
        self.region = region
        self.subdomain = subdomain
        self.experiment_type = experiment_type
        self.output_frequency = output_frequency
        self.grid_type = grid_type
        self.release = release

        # check kwarg input exists using data class
        cefi_data_path = portal_data.DataPath(
            region=region,
            subdomain=subdomain,
            experiment_type=experiment_type,
            output_frequency=output_frequency,
            grid_type=grid_type,
            release=release
        )

        self.cefi_rel_dir = cefi_data_path.cefi_dir
        self.cefi_local_dir = os.path.join(
            local_top_dir,
            self.cefi_rel_dir
        )

        # quick check on the top level directory
        top_level_dir = os.path.exists(os.path.join(
            local_top_dir,
            portal_data.DataStructure.top_directory[0]
        ))
        if not top_level_dir:
            raise FileNotFoundError('CEFI data structure does not exist at this location')

        # check if the release directory exist
        if  not os.path.exists(self.cefi_local_dir) and os.path.exists(os.path.dirname(self.cefi_local_dir)):
            parent_dir_release = os.path.dirname(self.cefi_local_dir)
            # List all directories under self.cefi_local_dir
            print('--------------------------------')
            print('Current release data is not valid. Available releases are:')
            for release_dir in os.listdir(parent_dir_release):
                release_path = os.path.join(parent_dir_release, release_dir)
                if os.path.isdir(release_path):
                    # prevent using the latest directory
                    if release_dir == 'latest':
                        pass
                    else:
                        print(release_dir)
            print('--------------------------------')
            raise FileNotFoundError('No files available based on release date')

        # check if data constructed by input exist
        if  not os.path.exists(self.cefi_local_dir):
            raise FileNotFoundError('Data structure constructed by input does not exist')


    def get_files(self,variable:Optional[str]=None)-> list:
        """Getting file in local storage

        Parameters
        ----------
        variable : str
            variable short name ex:'tos' for sea surface temperature
        
        Returns
        -------
        list
            a list of url in the form of string that 
            provide the locations of the data when
            accessing using opendap

        Raises
        ------
        FileNotFoundError
            When the files is empty that means the init setting 
            or code must have some incorrect pairing. Debug possibly 
            needed.
        """
        # include only netcdf file
        files = sorted(glob.glob(
            os.path.join(self.cefi_local_dir,'*.nc')
        ))

        filtered_files = []
        if variable is None:
            for file in files:
                filtered_files.append(file)
        else:
            for file in files:
                if variable == file.split('/')[-1].split('.')[0] :
                    filtered_files.append(file)

        # if zero file is found
        if not filtered_files :
            raise FileNotFoundError('No files available based on input')

        return filtered_files

class AccessFiles:
    """
    Frontend Class for user to get various mom6 simulation
    
    The class is designed to get the regional mom6 files
    by utilizing the OpenDapStore,LocalStore class to get the data
    """
    def __init__(
        self,
        region : ModelRegionOptions,
        subdomain : ModelSubdomainOptions,
        experiment_type : ModelExperimentTypeOptions,
        output_frequency : ModelOutputFrequencyOptions,
        grid_type : ModelGridTypeOptions,
        release : str,
        data_source : DataSourceOptions,
        local_top_dir : Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        region : ModelRegionOptions
            region name
        subdomain : ModelSubdomainOptions
            subdomain name
        experiment_type : ModelExperimentTypeOptions
            experiment type
        output_frequency : ModelOutputFrequencyOptions
            data output frequency
        grid_type : ModelGridTypeOptions
            model grid type
        release_date : str
            release date in the format of "rYYYYMMDD"
        data_source : DataSourceOptions
            'local', 'opendap', 's3'(unavailable), 'gcs'(unavailable)
        local_top_dir : str
            the absolution path to the local CEFI data.
            should be the absolute path before cefi_porta/...
        """
        self.storage = None

        # check data source
        if data_source == 'local':
            if local_top_dir is None:
                raise ValueError('local_top_dir is need to search for local data')
            else:
                self.storage = LocalStore(
                    local_top_dir,
                    region,
                    subdomain,
                    experiment_type,
                    output_frequency,
                    grid_type,
                    release
                )
        elif data_source == 'opendap':
            self.storage = OpenDapStore(
                region,
                subdomain,
                experiment_type,
                output_frequency,
                grid_type,
                release
            )
        elif data_source == 's3':
            self.storage = S3Store(
                region,
                subdomain,
                experiment_type,
                output_frequency,
                grid_type,
                release
            )
        elif data_source == 'gcs':
            self.storage = GCSStore(
                region,
                subdomain,
                experiment_type,
                output_frequency,
                grid_type,
                release
            )
        else :
            raise ValueError('only "local", "opendap", "s3", and "gcs" are available')

    def get(self,variable:Optional[str]=None, print_list=False)-> list:
        """Getting files from storage

        Parameters
        ----------
        variable : str
            variable short name ex:'tos' for sea surface temperature
        
        Returns
        -------
        list
            a list of url in the form of string that 
            provide the locations of the data when
            accessing using opendap

        """
        if self.storage:
            files = self.storage.get_files(variable)
            if print_list :
                print('--------- All available files ------------')
                for file in files:
                    print(file)
            return files
        else:
            raise FileNotFoundError('the storage is not assigned')