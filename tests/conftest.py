import datetime
import os
import pytest
import zipfile

import partridge as ptg

from fasttrips.Util import Util

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )
NETWORK_HOME_DIR = os.path.join(HOME_DIR, 'networks')

@pytest.fixture(scope="module", params=["simple", "psrc_1_1"])
def network(request):
    yield request.param

@pytest.fixture(scope="module")
def zip_file(network):
    network_dir = os.path.join(NETWORK_HOME_DIR, network)
    network_file = os.path.join(NETWORK_HOME_DIR, network + '.zip')
    with zipfile.ZipFile(network_file, 'w') as zipf:
        for root, dirs, files in os.walk(network_dir):
            for file in files:
                zipf.write(os.path.join(root, file), file)
    yield network_file
    os.unlink(network_file)


@pytest.fixture(scope="module")
def network_date(network):
    dates = {
        'simple': datetime.date(2015, 11, 22),
        'psrc_1_1': datetime.date(2016, 11, 22)
    }
    yield dates[network]

@pytest.fixture(scope="function")
def gtfs_feed(zip_file, network_date):
    from fasttrips.Assignment import Assignment
    Assignment.NETWORK_BUILD_DATE = network_date
    service_ids_by_date = ptg.read_service_ids_by_date(zip_file)
    service_ids = service_ids_by_date[network_date]
    feed = ptg.feed(os.path.join(zip_file),
                          config=Util.get_fast_trips_config(), view={
        'trips.txt': {
            'service_id': service_ids
        },
    })
    yield feed
