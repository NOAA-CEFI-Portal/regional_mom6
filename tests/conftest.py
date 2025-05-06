"""
This is a pytest setting file to have the option of 
pytesting under different location/platform

To do pytest on the local machine
`pytest` (will do the opendap option below)
`pytest --location opendap` (testing mock index calculation)
`pytest --location local` (testing real index calculation)

"""
import pytest

# Pytest hook function to add command line options
# This function is called before the test session starts
def pytest_addoption(parser):
    """add options for pytest command line arguments"""
    parser.addoption("--location", action="store", default="opendap")

# Pytest hook function to ignore certain tests
def pytest_ignore_collect(collection_path):
    """ignore the deprecated tests under the tests/deprecated folder
    """
    return "tests/deprecated" in str(collection_path)

# Define the fixture that loads `location` from pytest command line arguments
@pytest.fixture
def location(request):
    """get the `location` from pytest command line arguments
    used across test files
    """
    return request.config.getoption("--location")


def pytest_collection_modifyitems(config, items):
    """avoid running tests with "localonly" markers when not on local machine

    Parameters
    ----------
    config : 
        pytest config object
    items : _type_
        pytest items object includes all the tests
    location : _type_
        pytest fixture to get the location
    """
    
    location = config.getoption("--location")
    
    if location == "local":
        # Run all tests
        return

    # If location is not local, deselect tests with any custom marker
    deselected = []
    selected = []

    for item in items:
        if item.get_closest_marker("localonly"):
            deselected.append(item)
        else:
            selected.append(item)

    if deselected:
        # Pytest hook functions modify items in-place
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected
