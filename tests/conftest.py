import datetime
import os
import pytest

import partridge as ptg

from fasttrips.Util import Util

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )
NETWORK_HOME_DIR = os.path.join(HOME_DIR, 'networks')

@pytest.fixture(scope="module", params=["simple", "psrc_1_1"])
def network(request):
    yield request.param


@pytest.fixture(scope="module")
def network_date(network):
    dates = {
        'simple': datetime.date(2015, 11, 22),
        'psrc_1_1': datetime.date(2016, 11, 22)
    }
    yield dates[network]
