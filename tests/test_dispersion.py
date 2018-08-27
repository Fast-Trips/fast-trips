from __future__ import print_function
import os
import pytest
from fasttrips import Run

# TEST OPTIONS
test_thetas = [1.0, 0.5, 0.1]
test_size   = 5
disperson_rate_util_multiplier_factor = 10.0

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

@pytest.fixture(scope='module')
def utils_conversion_factor(dispersion_rate):
    factor = dispersion_rate*disperson_rate_util_multiplier_factor

    return factor

@pytest.mark.travis
def test_dispersion(dispersion_rate, utils_conversion_factor, passengers_arrived):

    r = Run.run_fasttrips(
        input_network_dir = INPUT_NETWORK,
        input_demand_dir  = INPUT_DEMAND,
        run_config        = os.path.join(INPUT_CONFIG,"config_ft.txt"),
        input_weights     = os.path.join(INPUT_CONFIG,"pathweight_ft.txt"),
        output_dir        = OUTPUT_DIR,
        output_folder     = "test_dispers_%4.2f" % dispersion_rate,
        max_stop_process_count = 2,
        utils_conversion_factor = utils_conversion_factor,
        pf_iters          = 2,
        overlap_variable  = "None",
        pathfinding_type  = "stochastic",
        iters             = 1,
        dispersion        = dispersion_rate,
        num_trips         = test_size )

    assert passengers_arrived == r["passengers_arrived"]

if __name__ == "__main__":
    for dr in test_thetas:
        util_factor = dr*disperson_rate_util_multiplier_factor
        print("Running test_dispersion.py with: disperson: %f1.2, util_factor: %f2.2, test_size: %d"  % (dr, util_factor, test_size))
        test_dispersion(dr, util_factor, test_size)
