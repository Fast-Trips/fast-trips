import datetime
import os

import pytest
import numpy as np
import pandas as pd

from fasttrips import Assignment
from fasttrips import PathSet
from fasttrips import Run

EXAMPLE_DIR    = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'Springfield')

# DIRECTORY LOCATIONS
INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'vermont')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'general')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'A')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_CONFIG, 'config_ft.txt')
INPUT_WEIGHTS       = os.path.join(INPUT_CONFIG, 'pathweight_ft.txt')

@pytest.mark.travis
@pytest.mark.pat
@pytest.mark.skip(reason="Not working - need to fix")
def test_pat_before_and_after():
    """
    Test to ensure that some of the pathfinder trips are returned before preferred departure
    or after preferred arrival.
    """
    OUTPUT_FOLDER       = 'pat_scenario'

    r = Run.run_fasttrips(
        input_network_dir    = INPUT_NETWORK,
        input_demand_dir     = INPUT_DEMAND,
        run_config           = CONFIG_FILE,
        input_weights        = INPUT_WEIGHTS,
        output_dir           = OUTPUT_DIR,
        output_folder        = OUTPUT_FOLDER,
        pathfinding_type     = "stochastic",
        overlap_variable     = "count",
        iters                = 1,
        dispersion           = 0.50
    )

    links = pd.read_csv(
        os.path.join(OUTPUT_DIR, OUTPUT_FOLDER, 'pathset_links.csv'),
        usecols=['person_trip_id','pathnum', 'linkmode', 'linknum','new_A_time','new_B_time'],
        parse_dates=['new_A_time', 'new_B_time'],
        infer_datetime_format=True
    )

    trips = pd.read_csv(
        os.path.join(INPUT_DEMAND, 'trip_list.txt'),
        usecols=['person_trip_id', 'departure_time', 'arrival_time', 'time_target']
    )

    departure_link = links.loc[links.groupby(['person_trip_id', 'pathnum'])['linknum'].idxmin()]
    #The C++ Pathfinder doesn't seem to respect the last egress leg from a preferred time perspective
    arrival_link = links.loc[links[links.linkmode == 'transit'].groupby(['person_trip_id', 'pathnum'])['linknum'].idxmax()]

    network_date = links['new_A_time'].dt.date.unique()[0]
    trips['arrival_time'] = trips['arrival_time'].apply(lambda x: parse_date(network_date, x))
    trips['departure_time'] = trips['departure_time'].apply(lambda x: parse_date(network_date, x))
    arrival_trips = trips[trips['time_target'] == 'arrival']
    departure_trips = trips[trips['time_target'] == 'departure']

    departures = pd.merge(
        departure_trips[['person_trip_id', 'departure_time']],
        departure_link[['person_trip_id', 'new_A_time']],
        on=['person_trip_id']
    )

    arrivals = pd.merge(
        arrival_trips[['person_trip_id', 'arrival_time']],
        arrival_link[['person_trip_id', 'new_B_time']],
        on=['person_trip_id']
    )

    early_departure = departures[departures['new_A_time'] < departures['departure_time']]
    size = early_departure.shape[0]
    assert size > 0
    confirm_size = early_departure[(early_departure['departure_time'] - early_departure['new_A_time']) / np.timedelta64(1, 'm') <= 10].shape[0]
    assert size == confirm_size

    late_arrivals = arrivals[arrivals['new_B_time'] > arrivals['arrival_time']]
    size = late_arrivals.shape[0]
    assert size > 0
    confirm_size = late_arrivals[(late_arrivals['new_B_time'] - late_arrivals['arrival_time']) / np.timedelta64(1, 'm') <= 10].shape[0]
    assert size == confirm_size


def test_pat_off():
    """
    Test to ensure that none of the pathfinder trips are returned before preferred departure
    or after preferred arrival.
    """

    OUTPUT_FOLDER  = 'pat_scenario_reg'

    r = Run.run_fasttrips(
        input_network_dir    = INPUT_NETWORK,
        input_demand_dir     = INPUT_DEMAND,
        run_config           = CONFIG_FILE,
        input_weights        = INPUT_WEIGHTS,
        output_dir           = OUTPUT_DIR,
        output_folder        = OUTPUT_FOLDER,
        pathfinding_type     = "stochastic",
        overlap_variable     = "None",
        iters                = 1,
        dispersion           = 0.50
    )

    links = pd.read_csv(
        os.path.join(OUTPUT_DIR, OUTPUT_FOLDER, 'pathset_links.csv'),
        usecols=['person_trip_id','pathnum', 'linkmode', 'linknum','new_A_time','new_B_time'],
        parse_dates=['new_A_time', 'new_B_time'],
        infer_datetime_format=True
    )

    trips = pd.read_csv(
        os.path.join(INPUT_DEMAND, 'trip_list.txt'),
        usecols=['person_trip_id', 'departure_time', 'arrival_time', 'time_target']
    )

    departure_link = links.loc[links.groupby(['person_trip_id', 'pathnum'])['linknum'].idxmin()]
    # The C++ Pathfinder doesn't seem to respect the last egress leg from a preferred time perspective
    arrival_link = links.loc[links[links.linkmode == 'transit'].groupby(['person_trip_id', 'pathnum'])['linknum'].idxmax()]

    network_date = links['new_A_time'].dt.date.unique()[0]
    trips['arrival_time'] = trips['arrival_time'].apply(lambda x: parse_date(network_date, x))
    trips['departure_time'] = trips['departure_time'].apply(lambda x: parse_date(network_date, x))
    arrival_trips = trips[trips['time_target'] == 'arrival']
    departure_trips = trips[trips['time_target'] == 'departure']

    departures = pd.merge(
        departure_trips[['person_trip_id', 'departure_time']],
        departure_link[['person_trip_id', 'new_A_time']],
        on=['person_trip_id']
    )

    arrivals = pd.merge(
        arrival_trips[['person_trip_id', 'arrival_time']],
        arrival_link[['person_trip_id', 'new_B_time']],
        on=['person_trip_id']
    )

    early_departure = departures[departures['new_A_time'] < departures['departure_time']]
    size = early_departure.shape[0]
    assert 0 == size
    confirm_size = early_departure[(early_departure['departure_time'] - early_departure['new_A_time']) / np.timedelta64(1, 'm') <= 10].shape[0]
    assert 0 == confirm_size

    late_arrivals = arrivals[arrivals['new_B_time'] > arrivals['arrival_time']]
    size = late_arrivals.shape[0]
    assert 0 == size
    confirm_size = late_arrivals[(late_arrivals['new_B_time'] - late_arrivals['arrival_time']) / np.timedelta64(1, 'm') <= 10].shape[0]
    assert 0 == confirm_size


def test_pat_growth_type_validation():
    PathSet.WEIGHTS_FIXED_WIDTH = True
    Assignment.read_weights(INPUT_WEIGHTS)
    check, error_str = PathSet.verify_weights(PathSet.WEIGHTS_DF)
    assert check

    sample = pd.DataFrame(data={
        'user_class': ['all'] * 7,
        'purpose': ['other'] * 7,
        'demand_mode_type': ['access'] * 7,
        'demand_mode': ['walk'] * 7,
        'supply_mode': ['walk_access'] * 7,
        'weight_name': ['depart_early_min'] * 7,
        'weight_value': list(np.random.rand(7)),
        'growth_type': ['linear'] * 2 + ['logarithmic'] * 2 + ['logistic'] * 3,
        'log_base': [np.nan] * 3 + list(np.random.rand(2)) + [np.nan] * 2,
        'logistic_mid': [np.nan] * 5 + [1.3] + [np.nan],
        'logistic_max': [16] + [np.nan] * 5 + [1.3],
    })

    check, error_str = PathSet.verify_weights(sample)
    assert not check

    expected_error = '\n-------Errors: pathweight_ft.txt---------------\n' \
        'Logistic qualifier includes log_base modifier\n' \
        'Logarithmic qualifier missing necessary log_base modifier\n' \
        'Logistic qualifier missing necessary modifiers\n'

    assert expected_error == error_str


def parse_date(datepart, timestring):
    return datetime.datetime.combine(
        datepart,
        datetime.datetime.strptime(timestring, '%H:%M:%S').time()
    )
