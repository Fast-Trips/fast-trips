import datetime
import os

import pandas as pd
from pyproj import Geod
import transitfeed

from fasttrips import Route
from fasttrips import Stop
from fasttrips import Trip

HOME_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )
NETWORK_HOME_DIR = os.path.join(HOME_DIR, 'networks', 'psrc_1_1')
TEST_HOME_DIR = os.path.join(HOME_DIR, 'test_trip_distance')
OUTPUT_DIR = os.path.join(TEST_HOME_DIR, 'output')


def test_trip_distance():
    build_date = datetime.date(2016, 11, 23)
    loader = transitfeed.Loader(NETWORK_HOME_DIR, memory_db=True)
    gtfs_schedule = loader.Load()

    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    stops = Stop(NETWORK_HOME_DIR, OUTPUT_DIR, gtfs_schedule, build_date)

    # Read routes, agencies, fares
    routes = Route(NETWORK_HOME_DIR, OUTPUT_DIR, gtfs_schedule, build_date, stops)

    # Read trips, vehicles, calendar and stoptimes
    trips = Trip(NETWORK_HOME_DIR, OUTPUT_DIR, gtfs_schedule, build_date, stops, routes, True)

    stop_times = trips.stop_times_df
    fast_trip_dist_idx = stop_times.groupby(['trip_id'])['shape_dist_traveled'].transform(max) == stop_times['shape_dist_traveled']
    fast_trip_dist = stop_times[fast_trip_dist_idx]

    trips_df = pd.read_csv(os.path.join(NETWORK_HOME_DIR, 'trips.txt'),
                           usecols=['trip_id', 'shape_id'])
    trips_df['trip_id'] = trips_df['trip_id'].astype(str)
    shapes_df = pd.read_csv(os.path.join(NETWORK_HOME_DIR, 'shapes.txt'),
                            usecols=['shape_id','shape_pt_lon', 'shape_pt_lat','shape_pt_sequence'])

    trips_df = pd.merge(trips_df, shapes_df, how='left', on='shape_id')


    itrip_df = trips_df.groupby(['trip_id'], group_keys=False).apply(compute_dist)
    gtfs_trip_dist_idx = itrip_df.groupby(['trip_id'])['shape_dist_traveled'].transform(max) == itrip_df['shape_dist_traveled']
    gtfs_trip_dist = itrip_df.loc[gtfs_trip_dist_idx,]

    diff = pd.merge(fast_trip_dist[['trip_id','shape_dist_traveled']],
                    gtfs_trip_dist[['trip_id','shape_dist_traveled']],
                    on='trip_id',suffixes=['_ft','_gtfs'])

    pd.testing.assert_series_equal(diff['shape_dist_traveled_ft'], diff['shape_dist_traveled_gtfs'],
                                   check_exact=False, check_less_precise=2, check_names=False)


def compute_dist(group):
    m_to_mi = 0.00062137
    geod = Geod(ellps='WGS84')
    group = group.sort_values('shape_pt_sequence')
    point_coords = group[['shape_pt_lon', 'shape_pt_lat']].values
    p_prev = point_coords[0]
    d = 0
    distances = [0]
    for p in point_coords[1:]:
        angle1, angle2, distance = geod.inv(p_prev[0], p_prev[1], p[0], p[1])
        d += distance * m_to_mi
        distances.append(d)
        p_prev = p
    group['shape_dist_traveled'] = distances
    return group
