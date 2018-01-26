import datetime
import os
import pytest
import zipfile

import partridge as ptg

from fasttrips.Util import Util

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )
NETWORK_HOME_DIR = os.path.join(HOME_DIR, 'networks')

@pytest.fixture(scope="module", params=["simple", "psrc_1_1"])
def scenario(request):
    yield request.param

@pytest.fixture(scope="module")
def zip_file(scenario):
    scenario_dir = os.path.join(NETWORK_HOME_DIR, scenario)
    scenario_file = os.path.join(NETWORK_HOME_DIR, scenario + '.zip')
    with zipfile.ZipFile(scenario_file, 'w') as zipf:
        for root, dirs, files in os.walk(scenario_dir):
            for file in files:
                zipf.write(os.path.join(root, file), file)
    yield scenario_file
    os.unlink(scenario_file)


@pytest.fixture(scope="module")
def scenario_date(scenario):
    dates = {
        'simple': datetime.date(2015, 11, 22),
        'psrc_1_1': datetime.date(2016, 11, 22)
    }
    yield dates[scenario]


@pytest.fixture(scope="function")
def gtfs_feed(zip_file, scenario_date):
    from fasttrips.Assignment import Assignment
    Assignment.NETWORK_BUILD_DATE = scenario_date
    service_ids_by_date = ptg.read_service_ids_by_date(zip_file)
    service_ids = service_ids_by_date[scenario_date]
    feed = ptg.feed(os.path.join(zip_file),
                          config=Util.get_fast_trips_config(), view={
        'trips.txt': {
            'service_id': service_ids
        },
    })
    yield feed
