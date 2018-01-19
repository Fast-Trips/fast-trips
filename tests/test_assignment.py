import os
import pytest

import partridge as ptg

import _fasttrips
from fasttrips import Trip
from fasttrips import Util

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples",)
TEST_HOME_DIR = os.path.join(HOME_DIR, 'test_assignment')
OUTPUT_DIR = os.path.join(TEST_HOME_DIR, 'output')

#This test is turned off until the supporting method is made static
#The underlying C++ code also does not start appropriately
@pytest.mark.skip(reason='Current implementation of Fast-Trips does not support static call to'
                         'Trip.add_shape_dist_travelled')
def test_assignment(zip_file, scenario_date):
    """
    Test of the fast_trips C++ assignment code. Send in OD pairs and time of day to test
    outputs.
    (Not fully implemented yet.)
    """
    service_ids_by_date = ptg.read_service_ids_by_date(zip_file)
    service_ids = service_ids_by_date[scenario_date]

    feed = ptg.feed(zip_file, view={
        'trips.txt': {
            'service_id': service_ids,
        },
    })

    stop_times_df = Trip.add_shape_dist_traveled(feed.stop_times, feed.stops)
    stop_times_df[Trip.SIM_COL_VEH_OVERCAP] = -1
    stop_times_df[Trip.STOPTIMES_COLUMN_TRIP_ID] = stop_times_df[Trip.STOPTIMES_COLUMN_TRIP_ID].astype('category')
    stop_times_df[Trip.STOPTIMES_COLUMN_STOP_ID] = stop_times_df[Trip.STOPTIMES_COLUMN_STOP_ID].astype('category')

    stop_times_df.rename(columns={
        Trip.STOPTIMES_COLUMN_ARRIVAL_TIME: Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
        Trip.STOPTIMES_COLUMN_DEPARTURE_TIME: Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
    }, inplace=True)

    # float version
    stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] = \
        stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] / 60

    stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] = \
        stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] / 60

    #stop_times_df[Trip.TRIPS_COLUMN_TRIP_ID_NUM] = stop_times_df[Trip.STOPTIMES_COLUMN_TRIP_ID].cat.codes + 1
    #stop_times_df[Trip.STOPTIMES_COLUMN_STOP_ID_NUM] =  stop_times_df[Trip.STOPTIMES_COLUMN_STOP_ID].cat.codes.values + 1
    trip_id_df = Util.add_numeric_column(feed.trips[[Trip.TRIPS_COLUMN_TRIP_ID]],
                                         id_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                                         numeric_newcolname=Trip.TRIPS_COLUMN_TRIP_ID_NUM)

    stop_times_df = Util.add_new_id(stop_times_df, Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                    mapping_df=trip_id_df,
                    mapping_id_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                    mapping_newid_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM)

    int32_inputs = stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].as_matrix().astype('int32')

    int64_inputs = stop_times_df[[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                                                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
                                                    Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED,
                                  Trip.SIM_COL_VEH_OVERCAP]].as_matrix().astype('float64')
    _fasttrips.reset()
    print stop_times_df.describe()
    _fasttrips.initialize_supply(OUTPUT_DIR,0,
                                 stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].as_matrix().astype('int32'),
                                 stop_times_df[[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                                                Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
                                                Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED,
                                                Trip.SIM_COL_VEH_OVERCAP]].as_matrix().astype('float64'))

    print 'initialized'


#_fasttrips.initialize_parameters(Assignment.TIME_WINDOW.total_seconds() / 60.0,
#                                 Assignment.BUMP_BUFFER.total_seconds() / 60.0,
#                                 Assignment.STOCH_PATHSET_SIZE,
#                                 Assignment.STOCH_DISPERSION,
#                                 Assignment.STOCH_MAX_STOP_PROCESS_COUNT,
#                                 1 if Assignment.TRANSFER_FARE_IGNORE_PATHFINDING else 0,
#                                 1 if Assignment.TRANSFER_FARE_IGNORE_PATHENUM else 0,
#                                 Assignment.MAX_NUM_PATHS,
#                                 Assignment.MIN_PATH_PROBABILITY)


#_fasttrips.find_pathset(iteration, pathfinding_iteration, hyperpath, pathset.person_id, pathset.person_trip_id,
#                        pathset.user_class, pathset.purpose, pathset.access_mode, pathset.transit_mode,
#                        pathset.egress_mode,
#                        pathset.o_taz_num, pathset.d_taz_num,
#                        1 if pathset.outbound else 0, float(pathset.pref_time_min), pathset.vot,
#                        1 if trace else 0)