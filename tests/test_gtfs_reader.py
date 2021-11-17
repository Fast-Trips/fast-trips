import datetime
import os
import pytest

import numpy as np
import pandas as pd
import partridge as ptg

from fasttrips import Route
from fasttrips import Stop
from fasttrips import Transfer
from fasttrips import Trip

EXAMPLE_DIR = os.path.join(os.getcwd(), 'fasttrips', 'Examples')

CONFIGS = [
    [os.path.join(EXAMPLE_DIR, 'Springfield', 'networks', 'vermont'), datetime.date(2015, 2, 3)],
]

def get_gtfs_feed(network, network_date):
    from fasttrips.Assignment import Assignment
    from fasttrips.Util import Util

    Assignment.NETWORK_BUILD_DATE = network_date

    service_ids_by_date = ptg.read_service_ids_by_date(network)
    service_ids = service_ids_by_date[network_date]
    feed = ptg.load_feed(network, config=Util.get_fast_trips_config(), view={
        'trips.txt': {'service_id': service_ids},
    })
    return feed

@pytest.mark.parametrize('network_dir, network_date', CONFIGS)
def test_stops_load(network_dir, network_date):
    '''
    Test to ensure that the Stops are loaded and processed
    '''
    gtfs_feed = get_gtfs_feed(network_dir, network_date)
    out_dir = os.path.join(EXAMPLE_DIR, 'output', 'test_gtfs_stops_load')
    try:
        os.makedirs(out_dir)
    except OSError:
        if not os.path.isdir(out_dir):
            raise

    stops = Stop(network_dir, out_dir, gtfs_feed, network_date)

    #Test existence, length, and required columns
    assert not stops.stop_id_df.empty
    assert len(stops.stop_id_df) == 8
    assert ({Stop.STOPS_COLUMN_STOP_ID, Stop.STOPS_COLUMN_STOP_ID_NUM}.issubset(stops.stop_id_df))
    assert stops.max_stop_id_num == 8

    #Test existence, length, dtype, and column names for stops_df
    assert not stops.stops_df.empty
    assert len(stops.stops_df == 8)
    stop_df_dtypes = {
        Stop.STOPS_COLUMN_STOP_ID: object,
        Stop.STOPS_COLUMN_STOP_LATITUDE: np.float64,
        Stop.STOPS_COLUMN_STOP_LONGITUDE: np.float64,
        Stop.STOPS_COLUMN_STOP_NAME: object,
        Stop.STOPS_COLUMN_ZONE_ID: object,
        Stop.STOPS_COLUMN_STOP_ID_NUM: np.int64,
        Stop.STOPS_COLUMN_ZONE_ID_NUM: np.float64
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

@pytest.mark.parametrize('network_dir, network_date', CONFIGS)
def test_routes_load(network_dir, network_date):
    """
        Test to ensure that the Routes are loaded and processed
        properly by the FastTrips initialization
    """
    gtfs_feed = get_gtfs_feed(network_dir, network_date)
    out_dir = os.path.join(EXAMPLE_DIR, 'output', 'test_gtfs_routes_load')
    try:
        os.makedirs(out_dir)
    except OSError:
        if not os.path.isdir(out_dir):
            raise

    stops = Stop(network_dir, out_dir,
                      gtfs_feed, network_date)

    routes = Route(network_dir, out_dir,
                      gtfs_feed, network_date, stops)

    #routes.routes_df
    assert not routes.routes_df.empty
    assert len(routes.routes_df) == 4
    routes_df_dtypes = {
        'route_id':    object,
        'route_long_name':    object,
        'route_short_name':    object,
        'route_type':     np.int64,
        'mode':     object,
        'proof_of_payment':     bool,
        'mode_num':     np.int64,
        'mode_type':     object,
        'route_id_num':     np.int64,
    }
    assert (set(routes_df_dtypes.keys()).issubset(routes.routes_df))
    assert routes.routes_df.dtypes.to_dict() == routes_df_dtypes
    assert len(routes.routes_df[routes.routes_df[Route.ROUTES_COLUMN_PROOF_OF_PAYMENT]]) == 1

    #routes.modes_df
    assert not routes.modes_df.empty
    assert len(routes.routes_df[routes.routes_df['mode'].isin(routes.modes_df['mode'])]) == len(routes.routes_df)
    pd.testing.assert_frame_equal(routes.routes_df[[Route.ROUTES_COLUMN_MODE, Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE_TYPE]].drop_duplicates().set_index(Route.ROUTES_COLUMN_MODE_NUM),
                                  routes.modes_df.set_index(Route.ROUTES_COLUMN_MODE_NUM))

    #routes.route_id_df
    assert not routes.route_id_df.empty
    assert len(routes.route_id_df) == len(routes.routes_df)
    pd.testing.assert_frame_equal(routes.routes_df[[Route.ROUTES_COLUMN_ROUTE_ID_NUM, Route.ROUTES_COLUMN_ROUTE_ID]].set_index(Route.ROUTES_COLUMN_ROUTE_ID_NUM),
                                  routes.route_id_df.set_index(Route.ROUTES_COLUMN_ROUTE_ID_NUM))

    #routes.agencies_df
    assert not routes.agencies_df.empty
    #The SIMPLE network doesn't really do much with agency.
    #TODO: Consider adding here once these tests includes more networks

    #routes.fare_rules_df
    assert not routes.fare_rules_df.empty


    #routes.fare_attrs_df
    assert not routes.fare_attrs_df.empty
    fare_attrs_df_dtype = {
        'fare_period': object,
        'price': np.float64,
        'currency_type': object,
        'payment_method': np.int64,
        'transfers': np.int64,
        'transfer_duration': np.float64,
    }
    assert (set(fare_attrs_df_dtype.keys()).issubset(routes.fare_attrs_df))
    assert routes.fare_attrs_df.dtypes.to_dict() == fare_attrs_df_dtype
    assert len(routes.fare_attrs_df) == 9
    match_fare_rule_attr_df = routes.fare_rules_df[routes.fare_rules_df[Route.FARE_ATTR_COLUMN_FARE_PERIOD].isin(routes.fare_attrs_df[Route.FARE_ATTR_COLUMN_FARE_PERIOD])]
    assert len(match_fare_rule_attr_df) == len(routes.fare_rules_df)
    #pd.testing.assert_frame_equal(routes.fare_rules_df[fare_attrs_df_dtype.keys()].drop_duplicates().set_index(Route.FARE_ATTR_COLUMN_FARE_PERIOD),
    #                              routes.fare_attrs_df[routes.fare_attrs_df[Route.FARE_ATTR_COLUMN_FARE_PERIOD].isin(match_fare_rule_attr_df[Route.FARE_ATTR_COLUMN_FARE_PERIOD])].set_index(Route.FARE_ATTR_COLUMN_FARE_PERIOD),
    #                              check_dtype=False, check_index_type=False)

    #routes.fare_ids_df
    assert not routes.fare_ids_df.empty
    pd.testing.assert_frame_equal(routes.fare_rules_df[[Route.FARE_RULES_COLUMN_FARE_ID, Route.FARE_RULES_COLUMN_FARE_ID_NUM]].drop_duplicates().set_index(Route.FARE_RULES_COLUMN_FARE_ID_NUM),
                                  routes.fare_ids_df.set_index(Route.FARE_RULES_COLUMN_FARE_ID_NUM))

    #routes.fare_by_class
    assert routes.fare_by_class

    #routes.fare_transfer_rules_df
    assert not routes.fare_transfer_rules_df.empty
    assert len(routes.fare_transfer_rules_df[routes.fare_transfer_rules_df[Route.FARE_TRANSFER_RULES_COLUMN_FROM_FARE_PERIOD]
        .isin(routes.fare_attrs_df[Route.FARE_ATTR_COLUMN_FARE_PERIOD])]) == len(routes.fare_transfer_rules_df)
    assert len(
        routes.fare_transfer_rules_df[routes.fare_transfer_rules_df[Route.FARE_TRANSFER_RULES_COLUMN_TO_FARE_PERIOD]
        .isin(routes.fare_attrs_df[Route.FARE_ATTR_COLUMN_FARE_PERIOD])]) == len(routes.fare_transfer_rules_df)


@pytest.mark.parametrize('network_dir, network_date', CONFIGS)
def test_transfers_load(network_dir, network_date):
    """
        Test to ensure that the Transfers are loaded and processed
        properly by the FastTrips initialization
    """
    gtfs_feed = get_gtfs_feed(network_dir, network_date)
    out_dir = os.path.join(EXAMPLE_DIR, 'output', 'test_gtfs_transfer_load')
    try:
        os.makedirs(out_dir)
    except OSError:
        if not os.path.isdir(out_dir):
            raise
    transfers = Transfer(network_dir, out_dir, gtfs_feed)

    assert not transfers.transfers_df.empty
    transfers_df_dtypes = {
        'min_transfer_time': np.float64,
        'from_stop_id': object,
        'to_stop_id': object,
        'transfer_type': np.float64,
        'stop2stop': bool,
        'dist': np.float64,
        'from_route_id': object,
        'to_route_id': object,
        'elevation_gain': np.int64,
        'schedule_precedence': object,
        'transfer_penalty': np.float64,
        'min_transfer_time_min': np.float64,
        'time_min': np.float64,
        'time': np.dtype('timedelta64[ns]'),
    }
    assert (set(transfers_df_dtypes.keys()).issubset(transfers.transfers_df))
    assert transfers.transfers_df.dtypes.to_dict() == transfers_df_dtypes
    assert len(transfers.transfers_df) == 6

    assert len(transfers.transfers_df[transfers.transfers_df['min_transfer_time'] > 0]) == 1


@pytest.mark.parametrize('network_dir, network_date', CONFIGS)
def test_trips_load(network_dir, network_date):
    """
        Test to ensure that the Trips are loaded and processed
        properly by the FastTrips initialization
    """
    from fasttrips import Assignment
    Assignment.NETWORK_BUILD_DATE = network_date
    gtfs_feed = get_gtfs_feed(network_dir, network_date)
    out_dir = os.path.join(EXAMPLE_DIR, 'output', 'test_gtfs_trips_load')
    try:
        os.makedirs(out_dir)
    except OSError:
        if not os.path.isdir(out_dir):
            raise

    stops = Stop(network_dir, out_dir, gtfs_feed, network_date)

    routes = Route(network_dir, out_dir, gtfs_feed, network_date, stops)

    trips = Trip(network_dir, out_dir, gtfs_feed, network_date, stops, routes, True)

    assert not trips.trips_df.empty
    assert len(trips.trips_df) == 153
    trips_df_dtypes = {
        'route_id': object, 'service_id': object, 'trip_id': object, 'vehicle_name': object,
        'direction_id': np.int64, 'shape_id': object, 'trip_id_num': np.int64,
        'seated_capacity': np.int64, 'standing_capacity': np.int64, 'max_speed': np.float64,
        'acceleration': np.float64, 'deceleration': np.float64, 'dwell_formula': object,
        'capacity': np.int64, 'max_speed_fps': np.float64, 'route_long_name': object,
        'route_short_name': object, 'route_type': np.int64, 'mode': object,
        'proof_of_payment': bool, 'mode_num': np.int64, 'mode_type': object,
        'route_id_num': np.int64, 'max_stop_seq': np.int64,
        'trip_departure_time': np.dtype('datetime64[ns]'),
    }
    assert (set(trips_df_dtypes.keys()).issubset(trips.trips_df))
    assert trips.trips_df.dtypes.to_dict() == trips_df_dtypes
    assert len(trips.trips_df[Route.ROUTES_COLUMN_ROUTE_ID].isin(routes.routes_df[Route.ROUTES_COLUMN_ROUTE_ID])) == \
        len(trips.trips_df)
    assert len(trips.trips_df[trips.trips_df.duplicated(subset=Trip.TRIPS_COLUMN_TRIP_ID)]) == 0

    pd.testing.assert_frame_equal(trips.trips_df[[
                Route.ROUTES_COLUMN_MODE, Route.ROUTES_COLUMN_MODE_NUM, Route.ROUTES_COLUMN_MODE_TYPE]]
                .drop_duplicates()
                .sort_values(by=Route.ROUTES_COLUMN_MODE_NUM)
                .set_index(Route.ROUTES_COLUMN_MODE_NUM),
        routes.modes_df.sort_values(by=Route.ROUTES_COLUMN_MODE_NUM).set_index(Route.ROUTES_COLUMN_MODE_NUM))

    pd.testing.assert_frame_equal(trips.trips_df[[
                Route.ROUTES_COLUMN_ROUTE_ID_NUM, Route.ROUTES_COLUMN_ROUTE_ID]]
                                  .drop_duplicates()
                                  .sort_values(by=Route.ROUTES_COLUMN_ROUTE_ID_NUM)
                                  .set_index(Route.ROUTES_COLUMN_ROUTE_ID_NUM),
                routes.route_id_df.sort_values(by=Route.ROUTES_COLUMN_ROUTE_ID_NUM).set_index(Route.ROUTES_COLUMN_ROUTE_ID_NUM))

    assert not trips.trip_id_df.empty
    pd.testing.assert_frame_equal(trips.trips_df[[
        Trip.TRIPS_COLUMN_TRIP_ID_NUM, Trip.TRIPS_COLUMN_TRIP_ID]]
                                  .drop_duplicates()
                                  .sort_values(by=Trip.TRIPS_COLUMN_TRIP_ID_NUM)
                                  .set_index(Trip.TRIPS_COLUMN_TRIP_ID_NUM),
                                  trips.trip_id_df.sort_values(by=Trip.TRIPS_COLUMN_TRIP_ID_NUM).set_index(
                                      Trip.TRIPS_COLUMN_TRIP_ID_NUM))

    assert not trips.stop_times_df.empty
    assert len(
        trips.stop_times_df[(trips.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].dt.month == network_date.month) &
                             (trips.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].dt.day == network_date.day) &
                            (trips.stop_times_df[
                                 Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].dt.year == network_date.year)]) == len(
        trips.stop_times_df)

    assert len(
        trips.stop_times_df[(trips.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].dt.month == network_date.month) &
                            (trips.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].dt.day == network_date.day) &
                            (trips.stop_times_df[
                                 Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].dt.year == network_date.year)]) == len(
        trips.stop_times_df)

    assert not trips.vehicles_df.empty

    assert trips.capacity_configured
