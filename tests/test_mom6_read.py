"""
Testing the module mom6_read
"""
import pytest
import requests
import io
from contextlib import redirect_stdout
from unittest.mock import patch, Mock
from mom6.mom6_module import mom6_read as mr


# Test fixtures for testing mom6_read
@pytest.fixture
def correct_arguments():
    return {
        'region' : 'northwest_atlantic',
        'subdomain' : 'full_domain',
        'experiment_type' : 'hindcast',
        'output_frequency' : 'monthly',
        'grid_type' : 'raw',
        'release' : 'r20230520'
    }
@pytest.fixture
def fail_region():
    return {
        'region' : 'northwest_atlantc',
        'subdomain' : 'full_domain',
        'experiment_type' : 'hindcast',
        'output_frequency' : 'monthly',
        'grid_type' : 'raw',
        'release' : 'r20230520'
    }
@pytest.fixture
def fail_subdomain():
    return {
        'region' : 'northwest_atlantic',
        'subdomain' : 'full_domai',
        'experiment_type' : 'hindcast',
        'output_frequency' : 'monthly',
        'grid_type' : 'raw',
        'release' : 'r20230520'
    }
@pytest.fixture
def fail_experiment_type():
    return {
        'region' : 'northwest_atlantic',
        'subdomain' : 'full_domain',
        'experiment_type' : 'hindcat',
        'output_frequency' : 'monthly',
        'grid_type' : 'raw',
        'release' : 'r20230520'
    }
@pytest.fixture
def fail_output_frequency():
    return {
        'region' : 'northwest_atlantic',
        'subdomain' : 'full_domain',
        'experiment_type' : 'hindcast',
        'output_frequency' : 'monthl',
        'grid_type' : 'raw',
        'release' : 'r20230520'
    }
@pytest.fixture
def fail_grid_type():
    return {
        'region' : 'northwest_atlantic',
        'subdomain' : 'full_domain',
        'experiment_type' : 'hindcast',
        'output_frequency' : 'monthly',
        'grid_type' : 'ra',
        'release' : 'r20230520'
    }
@pytest.fixture
def fail_release():
    return {
        'region' : 'northwest_atlantic',
        'subdomain' : 'full_domain',
        'experiment_type' : 'hindcast',
        'output_frequency' : 'monthly',
        'grid_type' : 'raw',
        'release' : 'r20230521'
    }

####### Test LocalStore Class #######
@pytest.mark.localonly
class TestLocalStore:
    """Test the LocalStore class"""
    def test_LocalStore_correct(self, correct_arguments):
        """test the LocalStore class"""
        result = mr.LocalStore(**correct_arguments,local_top_dir='/Projects/CEFI/regional_mom6/')
        # successful opendap store object creation
        assert isinstance(result, mr.LocalStore), f"Expected type LocalStore, but got {type(result)}"

    def test_LocalStore_fail_local_top_dir(self, correct_arguments):
        """test the LocalStore class"""
        with pytest.raises(FileNotFoundError):
            mr.LocalStore(**correct_arguments,local_top_dir='non_existent_directory')

    def test_LocalStore_fail_region(self, fail_region):
        """test the LocalStore class"""
        with pytest.raises(ValueError):
            mr.LocalStore(**fail_region,local_top_dir='/Projects/CEFI/regional_mom6/')
    def test_LocalStore_fail_subdomain(self, fail_subdomain):
        """test the LocalStore class"""
        with pytest.raises(ValueError):
            mr.LocalStore(**fail_subdomain,local_top_dir='/Projects/CEFI/regional_mom6/')
    def test_LocalStore_fail_experiment_type(self, fail_experiment_type):   
        """test the LocalStore class"""
        with pytest.raises(ValueError):
            mr.LocalStore(**fail_experiment_type,local_top_dir='/Projects/CEFI/regional_mom6/')
    def test_LocalStore_fail_output_frequency(self, fail_output_frequency):
        """test the LocalStore class"""
        with pytest.raises(ValueError):
            mr.LocalStore(**fail_output_frequency,local_top_dir='/Projects/CEFI/regional_mom6/')
    def test_LocalStore_fail_grid_type(self, fail_grid_type):
        """test the LocalStore class"""
        with pytest.raises(ValueError):
            mr.LocalStore(**fail_grid_type,local_top_dir='/Projects/CEFI/regional_mom6/')
    def test_LocalStore_fail_release(self, fail_release):
        """test the LocalStore class"""
        with pytest.raises(FileNotFoundError):
            mr.LocalStore(**fail_release,local_top_dir='/Projects/CEFI/regional_mom6/')


####### Test OpenDapStore Class #######
class TestOpenDapStore:
    """Test the OpenDapStore class"""
    def test_OpenDapStore_correct(self, correct_arguments):
        """test the OpenDapStore class"""
        result = mr.OpenDapStore(**correct_arguments)
        # successful opendap store object creation
        assert isinstance(result, mr.OpenDapStore), f"Expected type OpenDapStore, but got {type(result)}"
    def test_OpenDapStore_fail_region(self, fail_region):
        """test the OpenDapStore class"""
        with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_region)
    def test_OpenDapStore_fail_subdomain(self, fail_subdomain):
        """test the OpenDapStore class"""
        with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_subdomain)
    def test_OpenDapStore_fail_experiment_type(self, fail_experiment_type):   
        """test the OpenDapStore class"""
        with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_experiment_type)
    def test_OpenDapStore_fail_output_frequency(self, fail_output_frequency):
        """test the OpenDapStore class"""
        with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_output_frequency)
    def test_OpenDapStore_fail_grid_type(self, fail_grid_type):
        """test the OpenDapStore class"""
        with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_grid_type)
    def test_OpenDapStore_fail_release(self, fail_release):
        """test the OpenDapStore class"""
        with pytest.raises(FileNotFoundError):
            mr.OpenDapStore(**fail_release)

    def test_OpenDapStore_server_error(self, correct_arguments):
        """test the OpenDapStore class with server status error"""
        with patch("mom6.mom6_module.mom6_read.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 503
            mock_get.return_value = mock_response

            with pytest.raises(ConnectionError):
                mr.OpenDapStore(**correct_arguments)

    def test_OpenDapStore_server_connection_error(self, correct_arguments):
        """test the OpenDapStore class with server connection error"""
        with patch("mom6.mom6_module.mom6_read.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

            with pytest.raises(ConnectionError):
                mr.OpenDapStore(**correct_arguments)

    # Test OpenDapStore.get_file
    def test_OpenDapStore_get_file_correct(self, correct_arguments):
        """test the OpenDapStore class get file method"""
        store = mr.OpenDapStore(**correct_arguments)
        result = store.get_files()
        # successful file list object creation
        assert isinstance(result, list)

        result = store.get_files(variable='tos')
        # successful file list object creation
        assert isinstance(result, list)

    def test_OpenDapStore_get_file_fail_variable(self, correct_arguments):
        """test the OpenDapStore class get file method failed variable name"""
        store = mr.OpenDapStore(**correct_arguments)
        with pytest.raises(FileNotFoundError):
            store.get_files(variable='non_existent_variable')


####### Test GCSStore Class #######
class TestGCSStore:
    """Test the GCSStore class"""
    def test_GCSStore_correct(self, correct_arguments):
        """test the GCSStore class"""
        result = mr.GCSStore(**correct_arguments)
        # successful opendap store object creation
        assert isinstance(result, mr.GCSStore), f"Expected type GCSStore, but got {type(result)}"
    def test_GCSStore_fail_region(self, fail_region):
        """test the GCSStore class"""
        with pytest.raises(ValueError):
            mr.GCSStore(**fail_region)
    def test_GCSStore_fail_subdomain(self, fail_subdomain):
        """test the GCSStore class"""
        with pytest.raises(ValueError):
            mr.GCSStore(**fail_subdomain)
    def test_GCSStore_fail_experiment_type(self, fail_experiment_type):   
        """test the GCSStore class"""
        with pytest.raises(ValueError):
            mr.GCSStore(**fail_experiment_type)
    def test_GCSStore_fail_output_frequency(self, fail_output_frequency):
        """test the OpenDGCSStoreapStore class"""
        with pytest.raises(ValueError):
            mr.GCSStore(**fail_output_frequency)
    def test_GCSStore_fail_grid_type(self, fail_grid_type):
        """test the GCSStore class"""
        with pytest.raises(ValueError):
            mr.GCSStore(**fail_grid_type)
    def test_GCSStore_fail_release(self, fail_release):
        """test the GCSStore class"""
        with pytest.raises(FileNotFoundError):
            mr.GCSStore(**fail_release)

    # Test OpenDapStore.get_file
    def test_GCSStore_get_file_correct(self, correct_arguments):
        """test the GCSStore class get file method"""
        store = mr.GCSStore(**correct_arguments)
        result = store.get_files()
        # successful file list object creation
        assert isinstance(result, list)

        result = store.get_files(variable='tos')
        # successful file list object creation
        assert isinstance(result, list)

    def test_GCSStore_get_file_fail_variable(self, correct_arguments):
        """test the GCSStore class get file method failed variable name"""
        store = mr.GCSStore(**correct_arguments)
        with pytest.raises(FileNotFoundError):
            store.get_files(variable='non_existent_variable')

####### Test AccessFiles Class #######
class test_AccessFiles:
    """Test the AccessFiles class"""
    def test_AccessFiles_correct_opendap(self, correct_arguments):
        """test the AccessFiles class"""
        result = mr.AccessFiles(**correct_arguments,data_source='opendap')
        # successful access files object creation
        assert isinstance(result, mr.AccessFiles), f"Expected type AccessFiles, but got {type(result)}"

    def test_AccessFiles_correct_gcs(self, correct_arguments):
        """test the AccessFiles class"""
        result = mr.AccessFiles(**correct_arguments,data_source='gcs')
        # successful access files object creation
        assert isinstance(result, mr.AccessFiles), f"Expected type AccessFiles, but got {type(result)}"

    def test_AccessFiles_correct_s3(self, correct_arguments):
        """test the AccessFiles class"""
        result = mr.AccessFiles(**correct_arguments,data_source='s3')
        # successful access files object creation
        assert isinstance(result, mr.AccessFiles), f"Expected type AccessFiles, but got {type(result)}"

    def test_AccessFiles_correct_opendap_get(self, correct_arguments):
        """test the AccessFiles get method"""
        store = mr.AccessFiles(**correct_arguments,data_source='opendap')

        f = io.StringIO()
        
        with redirect_stdout(f):
            result_list = store.get(print_list=True)
            assert isinstance(result_list, list), f"Expected type list, but got {type(result_list)}"
        
        print_lines = f.getvalue()
        
        # Check that something was printed
        assert print_lines.strip() != ""

        # check that number of printed lines -1 (header line) matches list length
        lines = print_lines.strip().split('\n')
        assert len(lines)-1 == len(result_list)

    def test_AccessFiles_fail_datasource(self, correct_arguments):
        """test the AccessFiles class for invalid data source"""
        with pytest.raises(ValueError):
            mr.AccessFiles(**correct_arguments,data_source='aws')

    def test_AccessFiles_fail_local(self, correct_arguments):
        """test the AccessFiles class for wrong or no local_top_dir"""
        # should raise FileNotFoundError from the LocalStore class
        with pytest.raises(FileNotFoundError):
            mr.AccessFiles(**correct_arguments,data_source='local', local_top_dir='non_existent_directory')
        with pytest.raises(ValueError):
            mr.AccessFiles(**correct_arguments,data_source='local')


