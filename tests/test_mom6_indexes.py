#!/usr/bin/env python

"""
Testing the module mom6_indexes

For local testing:
`pytest --location=local` 


The location option is implemented due to the conftest.py

"""
import pytest
import numpy as np
from mom6.mom6_module import mom6_indexes
from mom6.mom6_module.mom6_io import MOM6Historical


def test_gulf_stream_index(location):
    """testing the gulf stream index calculation

    Parameters
    ----------
    location : str
        The location of where the data is extracted.

    Raises
    ------
    ValueError
        The location input in string does not exist.
    """
    if location == 'local':
        ds_test = MOM6Historical('ssh','hist_run/','static/','raw',location).get_all()
        ds_test = ds_test.rename({'geolon':'lon','geolat':'lat'})
    elif location == 'opendap':
        try:
            ds_test = MOM6Historical(var='ssh',source=location).get_all()
        except OSError :
            pytest.fail('OSError is raised when access OPeNDAP data')
    else :
        raise ValueError(
            f'the input --location={location} '+
            'does not exist. Please put "local" or "opendap".'
        )
    if location == 'local':
        mom_gfi = mom6_indexes.GulfStreamIndex(ds_test,'ssh')
        ds_gs = mom_gfi.generate_index()
        # whole dataset examination
        assert np.abs(np.abs(ds_gs.gulf_stream_index).sum().compute().data - 264.06818) < 1e-5
        assert np.abs(ds_gs.gulf_stream_index.max().compute().data - 2.5614245) < 1e-6
        assert np.abs(ds_gs.gulf_stream_index.min().compute().data - -2.5407326) < 1e-6
        
    # elif location == 'opendap':
    #     # only two years of data
    #     assert np.abs(np.abs(ds_gs.gulf_stream_index).sum().compute().data - 20.642387) < 1e-5
    #     assert np.abs(ds_gs.gulf_stream_index.max().compute().data - 1.7084963) < 1e-6
    #     assert np.abs(ds_gs.gulf_stream_index.min().compute().data - -1.7084963) < 1e-6
