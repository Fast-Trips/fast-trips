import datetime
import os
import pytest

import partridge as ptg

from fasttrips.Util import Util

NETWORKS = [("Springfield","VT",datetime.date(2015, 11, 22)),
            ("Seattle_Region","psrc_1_1",datetime.date(2016, 11, 22)),
           ]

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )

@pytest.fixture(scope="function")
def gtfs_feed(network):
    from fasttrips.Assignment import Assignment

    Assignment.NETWORK_BUILD_DATE = network[2]
    network_dir = os.path.join(HOME_DIR, network[0], "networks", network[1])

    service_ids_by_date = ptg.read_service_ids_by_date(network_dir)
    service_ids         = service_ids_by_date[network_date]
    feed                = ptg.feed(network_dir,
                          config=Util.get_fast_trips_config(), view={
            'trips.txt': {
            'service_id': service_ids
        },
    })
    yield feed


@pytest.fixture(scope="module", params=NETWORKS)
def network(request):
    yield request.param
