import pytest

def pytest_addoption(parser):
    parser.addoption("--location", action="store", default="opendap")
    parser.addoption("--data", action="store", default="test")
    parser.addoption("--static", action="store", default="")

@pytest.fixture
def location(request):
    return request.config.getoption("--location")
@pytest.fixture
def data_file(request):
    return request.config.getoption("--data")
@pytest.fixture
def static_data_file(request):
    return request.config.getoption("--static")
