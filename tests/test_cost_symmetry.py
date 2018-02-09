import os
import pytest

import pandas as pd

import _fasttrips
from fasttrips import Assignment
from fasttrips import FastTrips
from fasttrips import Passenger
from fasttrips import PathSet
from fasttrips import Trip


EXAMPLES_DIR = os.path.join(os.getcwd(), 'fasttrips', 'Examples')


@pytest.fixture(scope='module', params=['demand_reg', 'demand_pat'])
def demand_folder(request):
    """
    Grab the right input folders for the test.
    """
    return request.param


@pytest.fixture(scope='module')
def ft_instance(demand_folder):
    """
    The tests need a Fast-Trips instance. This is shared code for each demand folder under test.
    """

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, 'networks', 'simple')
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, 'demand', demand_folder)
    OUTPUT_FOLDER = os.path.join(EXAMPLES_DIR, 'output', 'test_assignment', demand_folder)

    try:
        os.makedirs(OUTPUT_FOLDER)
    except OSError:
        if not os.path.isdir(OUTPUT_FOLDER):
            raise

    ft = FastTrips(
        INPUT_NETWORK,
        INPUT_DEMAND,
        os.path.join(INPUT_DEMAND, 'pathweight_ft.txt'),
        os.path.join(INPUT_DEMAND, 'config_ft.txt'),
        OUTPUT_FOLDER
    )

    ft.read_configuration()
    ft.read_input_files()

    Assignment.PATHFINDING_TYPE = Assignment.PATHFINDING_TYPE_STOCHASTIC
    Assignment.STOCH_DISPERSION = 0.5

    _fasttrips.reset()

    yield ft


@pytest.fixture(scope='module')
def pathfinder_paths(ft_instance, demand_folder):
    """
    Generate the C++ pathfinder results for a set of demand inputs. This method yields
    results, so that it could potentially be recycled for multiple tests.
    :return: (pf_pathset_paths, pf_pathset_links)
    """

    ft = ft_instance
    OUTPUT_FOLDER = os.path.join(EXAMPLES_DIR, 'output', 'test_assignment', demand_folder)

    veh_trips_df = ft.trips.get_full_trips()

    Trip.reset_onboard(veh_trips_df)

    Assignment.initialize_fasttrips_extension(0, OUTPUT_FOLDER, veh_trips_df)

    # This statement looks weird, but it is required later when setting up passenger pathsets
    ft.passengers.pathfind_trip_list_df = ft.passengers.trip_list_df

    path_cols = list(ft.passengers.pathfind_trip_list_df.columns.values)
    for path_tuple in ft.passengers.pathfind_trip_list_df.itertuples(index=False):
        path_dict = dict(zip(path_cols, path_tuple))
        trip_list_id = path_dict[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]

        trip_pathset = PathSet(path_dict)
        ft.passengers.add_pathset(trip_list_id, trip_pathset)

        (pathdict, perf_dict) = \
            Assignment.find_trip_based_pathset(1, 1, trip_pathset,
                                               Assignment.PATHFINDING_TYPE ==
                                               Assignment.PATHFINDING_TYPE_STOCHASTIC,
                                               trace=False)

        trip_pathset.pathdict = pathdict

    yield ft.passengers.setup_passenger_pathsets(1, 1, ft.stops, ft.trips.trip_id_df,
                                                  ft.trips.trips_df, ft.routes.modes_df,
                                                  ft.transfers, ft.tazs,
                                                  Assignment.PREPEND_ROUTE_ID_TO_TRIP_ID)


@pytest.fixture(scope='module')
def simulation_paths(ft_instance, pathfinder_paths):
    """
    Generate the assignment simulation calculate_cost results for a set of demand inputs. This
    method yields results, so that it could potentially be recycled for multiple tests.
    :return: (sim_pathset_paths, sim_pathset_links)
    """

    ft = ft_instance

    pf_pathset_paths = pathfinder_paths[0]
    pf_pathset_links = pathfinder_paths[1]

    veh_trips_df = ft.trips.get_full_trips()

    sim_pathset_links = pf_pathset_links.copy()
    sim_pathset_paths = pf_pathset_paths.copy()

    sim_pathset_links = Assignment.find_passenger_vehicle_times(sim_pathset_links, veh_trips_df)
    (sim_pathset_paths, sim_pathset_links) = Assignment.flag_missed_transfers(sim_pathset_paths,
                                                                              sim_pathset_links)

    yield PathSet.calculate_cost(
        Assignment.STOCH_DISPERSION, sim_pathset_paths, sim_pathset_links, veh_trips_df,
        ft.passengers.trip_list_df, ft.routes, ft.tazs, ft.transfers, stops=ft.stops,
        reset_bump_iter=False
    )


def test_cost_symmetry(pathfinder_paths, simulation_paths):
    paths_join_col = ['trip_list_id_num', 'pathnum']
    links_join_col = paths_join_col + ['linknum']

    pf_pathset_paths = pathfinder_paths[0]
    pf_pathset_links = pathfinder_paths[1]

    sim_pathset_paths = simulation_paths[0]
    sim_pathset_links = simulation_paths[1]

    # First quick test to ensure that PF and SIM return the same number of records
    assert pf_pathset_paths.shape[0] == sim_pathset_paths.shape[0]
    assert pf_pathset_links.shape[0] == sim_pathset_links.shape[0]

    paths = pd.merge(pf_pathset_paths[paths_join_col + ['pf_cost']],
                     sim_pathset_paths[paths_join_col + ['sim_cost']],
                     on=paths_join_col,
                     suffixes=['pf','sim'])
    links = pd.merge(pf_pathset_links[links_join_col + ['pf_linkcost']],
                     sim_pathset_links[links_join_col + ['sim_cost']],
                     on=links_join_col,
                     suffixes=['pf','sim'])

    # Assert that the join resulted in the same number of records
    assert pf_pathset_paths.shape[0] == paths.shape[0]
    assert pf_pathset_links.shape[0] == links.shape[0]

    # Assert that all of the pathfinding costs equal the sim costs
    pd.testing.assert_series_equal(paths['pf_cost'], paths['sim_cost'],
                                   check_names=False, check_less_precise=5)
    pd.testing.assert_series_equal(links['pf_linkcost'], links['sim_cost'],
                                   check_names=False, check_less_precise=5)
