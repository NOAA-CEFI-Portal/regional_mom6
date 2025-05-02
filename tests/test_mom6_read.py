
"""
Testing the module mom6_read
"""
import pytest
import requests
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

####### Test OpenDapStore Class #######
def test_OpenDapStore_correct(correct_arguments):
    """test the OpenDapStore class"""
    result = mr.OpenDapStore(**correct_arguments)
    # successful opendap store object creation
    assert isinstance(result, mr.OpenDapStore), f"Expected type OpenDapStore, but got {type(result)}"
def test_OpenDapStore_fail_region(fail_region):
    """test the OpenDapStore class"""
    with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_region)
def test_OpenDapStore_fail_subdomain(fail_subdomain):
    """test the OpenDapStore class"""
    with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_subdomain)
def test_OpenDapStore_fail_experiment_type(fail_experiment_type):   
    """test the OpenDapStore class"""
    with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_experiment_type)
def test_OpenDapStore_fail_output_frequency(fail_output_frequency):
    """test the OpenDapStore class"""
    with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_output_frequency)
def test_OpenDapStore_fail_grid_type(fail_grid_type):
    """test the OpenDapStore class"""
    with pytest.raises(ValueError):
            mr.OpenDapStore(**fail_grid_type)
def test_OpenDapStore_fail_release(fail_release):
    """test the OpenDapStore class"""
    with pytest.raises(FileNotFoundError):
            mr.OpenDapStore(**fail_release)

def test_OpenDapStore_server_error(correct_arguments):
    """test the OpenDapStore class with server status error"""
    with patch("mom6.mom6_module.mom6_read.requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 503
        mock_get.return_value = mock_response

        with pytest.raises(ConnectionError):
            mr.OpenDapStore(**correct_arguments)

def test_OpenDapStore_server_connection_error(correct_arguments):
    """test the OpenDapStore class with server connection error"""
    with patch("mom6.mom6_module.mom6_read.requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with pytest.raises(ConnectionError):
            mr.OpenDapStore(**correct_arguments)

# Test OpenDapStore.get_file
def test_OpenDapStore_get_file_correct(correct_arguments):
    """test the OpenDapStore class get file method"""
    store = mr.OpenDapStore(**correct_arguments)
    result = store.get_files()
    # successful file list object creation
    assert isinstance(result, list)

    result = store.get_files(variable='tos')
    # successful file list object creation
    assert isinstance(result, list)

def test_OpenDapStore_get_file_fail_variable(correct_arguments):
    """test the OpenDapStore class get file method failed variable name"""
    store = mr.OpenDapStore(**correct_arguments)
    with pytest.raises(FileNotFoundError):
        store.get_files(variable='non_existent_variable')


####### Test AccessFiles Class #######