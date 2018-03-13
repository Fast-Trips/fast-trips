import datetime
import os
import pytest

import partridge as ptg

from fasttrips.Util import Util

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )
NETWORK_HOME_DIR = os.path.join(HOME_DIR, 'networks')


@pytest.fixture(scope="function")
def gtfs_feed(network, network_date):
    from fasttrips.Assignment import Assignment
    Assignment.NETWORK_BUILD_DATE = network_date
    network_dir = os.path.join(NETWORK_HOME_DIR, network)

    service_ids_by_date = ptg.read_service_ids_by_date(network_dir)
    service_ids = service_ids_by_date[network_date]
    feed = ptg.feed(network_dir,
                          config=Util.get_fast_trips_config(), view={
        'trips.txt': {
            'service_id': service_ids
        },
    })
    yield feed


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
