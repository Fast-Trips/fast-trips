import os
import pytest
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

# TEST PARAMETERS
test_mspc = [10, 50, 100]
test_size   = 5

@pytest.fixture(scope='module', params=test_mspc)
def stop_process_count(request):
    return request.param


@pytest.fixture(scope='module')
def passengers_arrived(stop_process_count):
    arrived = dict(zip(test_mspc,[test_size]*len(test_mspc)))

    return arrived[stop_process_count]


def test_max_stop_process_count(stop_process_count, passengers_arrived):

    r = Run.run_fasttrips(
        input_network_dir = INPUT_NETWORK,
        input_demand_dir  = INPUT_DEMAND,
        run_config        = CONFIG_FILE,
        input_weights     = INPUT_WEIGHTS,
        output_dir        = OUTPUT_DIR,
        output_folder     = "test_dispers_%4.2d" % stop_process_count,
        overlap_variable  = "None",
        pf_iters          = 2,
        pathfinding_type  = "stochastic",
        max_stop_process_count = stop_process_count,
        iters             = 1,
        num_trips         = test_size,
        dispersion        = 0.50 )

    assert passengers_arrived == r["passengers_arrived"]
