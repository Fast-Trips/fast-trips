import os
import pytest
from fasttrips import Run

# TEST OPTIONS
test_thetas = [1.0, 0.5, 0.1]
test_size   = 5

# DIRECTORY LOCATIONS
EXAMPLE_DIR         = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'Springfield')

INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'vermont')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'general')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'A')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')

@pytest.fixture(scope='module', params=test_thetas)
def dispersion_rate(request):
    return request.param

@pytest.fixture(scope='module')
def passengers_arrived(dispersion_rate):
    arrived = dict(zip(test_thetas,[test_size]*len(test_thetas)))

    return arrived[dispersion_rate]

@pytest.mark.travis
def test_dispersion(dispersion_rate, passengers_arrived):

    r = Run.run_fasttrips(
        input_network_dir = INPUT_NETWORK,
        input_demand_dir  = INPUT_DEMAND,
        run_config        = os.path.join(INPUT_CONFIG,"config_ft.txt"),
        input_weights     = os.path.join(INPUT_CONFIG,"pathweight_ft.txt"),
        output_dir        = OUTPUT_DIR,
        output_folder     = "test_dispers_%4.2f" % dispersion_rate,
        max_stop_process_count = 2,
        pf_iters          = 2,
        overlap_variable  = "None",
        pathfinding_type  = "stochastic",
        iters             = 1,
        dispersion        = dispersion_rate,
        num_trips         = test_size )

    assert passengers_arrived == r["passengers_arrived"]
