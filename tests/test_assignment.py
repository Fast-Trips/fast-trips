import datetime
import os

import numpy as np
import partridge as ptg
import pytest

import _fasttrips
from fasttrips import Trip


class TestFastTripsAssignment(object):
    HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples",)
    NETWORK_HOME_DIR = os.path.join(HOME_DIR, 'networks')
    TEST_HOME_DIR = os.path.join(HOME_DIR, 'test_assignment')
    OUTPUT_DIR = os.path.join(TEST_HOME_DIR, 'output')

    process_number = 1

    def stop_times(self):
        path = os.path.join(self.NETWORK_HOME_DIR, 'simple_gtfs.zip')
        service_ids_by_date = ptg.read_service_ids_by_date(path)
        service_ids = service_ids_by_date[datetime.date(2015, 11, 22)]

        feed = ptg.feed(path, view={
            'trips.txt' : {
              'service_id': service_ids,
            },
        })

        stop_times_df = feed.stop_times

        stop_times_df.rename(columns={
            Trip.STOPTIMES_COLUMN_ARRIVAL_TIME: Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
            Trip.STOPTIMES_COLUMN_DEPARTURE_TIME: Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
        }, inplace=True)

        # float version
        stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] = \
            stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] / 60

        stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] = \
            stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] / 60

        return stop_times_df

    def test_assignment(self):
        stop_times = self.stop_times();
        stop_times[Trip.SIM_COL_VEH_OVERCAP] = 0
        stop_times[Trip.STOPTIMES_COLUMN_TRIP_ID] = stop_times[Trip.STOPTIMES_COLUMN_TRIP_ID].astype('category')
        stop_times[Trip.STOPTIMES_COLUMN_STOP_ID] = stop_times[Trip.STOPTIMES_COLUMN_STOP_ID].astype('category')


        paths = np.array([
            stop_times[Trip.STOPTIMES_COLUMN_TRIP_ID].cat.codes,
            stop_times[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE].astype('int32'),
            stop_times[Trip.STOPTIMES_COLUMN_STOP_ID].cat.codes.values,
        ]).transpose()

        time_distance = stop_times[
            [
                Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
                Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED,
                Trip.SIM_COL_VEH_OVERCAP
            ]
        ].values.astype('int64')

        _fasttrips.initialize_supply(TestFastTripsAssignment.OUTPUT_DIR,
                                     1,
                                     paths,
                                     time_distance)


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