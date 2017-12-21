import datetime
import os
import pytest

import numpy as np
import pandas as pd
import transitfeed

from fasttrips import Stop

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )
NETWORK_HOME_DIR = os.path.join(HOME_DIR, 'networks')



@pytest.fixture(scope="module", params=["simple"])
def gtfs_schedule(request):
    loader = transitfeed.Loader(os.path.join(NETWORK_HOME_DIR, request.param), memory_db=True)
    yield loader.Load()

#@pytest.fixture(scope="module", params=["simple"])
#def gtfs_build_date:
#build_date = datetime.date(2016, 11, 23)


def test_stops_load(gtfs_schedule):
    stops = Stop(NETWORK_HOME_DIR, os.path.join(HOME_DIR, 'test_gtfs_objects'),
                      gtfs_schedule, datetime.date(2015, 11, 23))

    #Test existence, length, and required columns
    assert not stops.stop_id_df.empty
    assert len(stops.stop_id_df) == 8
    assert ({Stop.STOPS_COLUMN_STOP_ID, Stop.STOPS_COLUMN_STOP_ID_NUM}.issubset(stops.stop_id_df))
    assert stops.max_stop_id_num == 8

    #Test existence, length, dtype, and column names for stops_df
    assert not stops.stops_df.empty
    assert len(stops.stops_df == 8)
    stop_df_dtypes = {
        'location_type': np.int64,
        'stop_id': object,
        'stop_lat': np.float64,
        'stop_lon': np.float64,
        'stop_name': object,
        'zone_id': object,
        'stop_id_num': np.int64,
        'zone_id_num': np.float64
    }
    assert (set(stop_df_dtypes.keys()).issubset(stops.stops_df))
    assert stops.stops_df.dtypes.to_dict() == stop_df_dtypes
    pd.testing.assert_frame_equal(stops.stop_id_df.set_index(Stop.STOPS_COLUMN_STOP_ID_NUM),
                                  stops.stops_df.set_index(Stop.STOPS_COLUMN_STOP_ID_NUM),
    )

    #Test to make sure stop_id and stops dataframe are identical
    stops_with_zones = stops.stops_df[stops.stops_df[Stop.STOPS_COLUMN_ZONE_ID_NUM].notnull()]
    assert len(stops_with_zones) == 2
    assert len(stops_with_zones[stops_with_zones[Stop.STOPS_COLUMN_STOP_ID].isin(['B1','B3'])]) == 2


    assert len(stops.zone_id_df) == 2
    pd.testing.assert_frame_equal(stops_with_zones[[Stop.STOPS_COLUMN_ZONE_ID_NUM, Stop.STOPS_COLUMN_ZONE_ID]].drop_duplicates().set_index(Stop.STOPS_COLUMN_ZONE_ID_NUM),
        stops.zone_id_df.set_index(Stop.STOPS_COLUMN_ZONE_ID_NUM), check_dtype=False, check_index_type=False)

    assert not stops.trip_times_df