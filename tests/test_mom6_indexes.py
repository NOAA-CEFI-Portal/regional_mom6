import warnings
import numpy as np
from mom6.mom6_module import mom6_indexes

warnings.simplefilter("ignore")
def test_gulf_stream_index():
    GFI = mom6_indexes.GulfStreamIndex(
        data_type='historical',
        grid='raw')
    ds = GFI.generate_index()
    
    # print(np.abs(ds.gulf_stream_index).sum().compute().data)
    assert np.abs(np.abs(ds.gulf_stream_index).sum().compute().data - 264.06818) < 1e-5
    assert np.abs(ds.gulf_stream_index.max().compute().data - 2.5614245) < 1e-7
    assert np.abs(ds.gulf_stream_index.min().compute().data - -2.5407326) < 1e-7

