import datetime
import os

import numpy as np
import pandas as pd

from fasttrips import Run


def test_pat_before_and_after():
    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_pat")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")
    OUTPUT_FOLDER  = 'pat_scenario'

    r = Run.run_fasttrips(
        input_network_dir    = INPUT_NETWORK,
        input_demand_dir     = INPUT_DEMAND,
        run_config           = os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_weights        = os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
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
    assert 576 == size
    confirm_size = early_departure[(early_departure['departure_time'] - early_departure['new_A_time']) / np.timedelta64(1, 'm') <= 10].shape[0]
    assert size == confirm_size

    late_arrivals = arrivals[arrivals['new_B_time'] > arrivals['arrival_time']]
    size = late_arrivals.shape[0]
    assert 251 == size
    confirm_size = late_arrivals[(late_arrivals['new_B_time'] - late_arrivals['arrival_time']) / np.timedelta64(1, 'm') <= 10].shape[0]
    assert size == confirm_size


def test_pat_off():
    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")
    OUTPUT_FOLDER  = 'pat_scenario_reg'

    r = Run.run_fasttrips(
        input_network_dir    = INPUT_NETWORK,
        input_demand_dir     = INPUT_DEMAND,
        run_config           = os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_weights        = os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
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


def parse_date(datepart, timestring):
    return datetime.datetime.combine(
        datepart,
        datetime.datetime.strptime(timestring, '%H:%M:%S').time()
    )
