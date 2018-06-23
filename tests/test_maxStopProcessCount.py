import os
import pytest
from fasttrips import Run

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

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        overlap_variable = "None",
        output_folder     = "test_dispers_%4.2d" % stop_process_count,
        pf_iters         = 2,
        pathfinding_type  = "stochastic",
        max_stop_process_count = stop_process_count,
        iters             = 1,
        num_trips         = test_size ,
        dispersion        = 0.50 )

    assert passengers_arrived == r["passengers_arrived"]
