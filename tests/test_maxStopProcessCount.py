import os
import pytest

from fasttrips import Run


@pytest.fixture(scope='module', params=[10, 50, 100])
def stop_process_count(request):
    return request.param


@pytest.fixture(scope='module')
def passengers_arrived(stop_process_count):
    arrived = {
        10: 726,
        50: 726,
        100: 726,
    }

    return arrived[stop_process_count]


def test_max_stop_process_count(stop_process_count, passengers_arrived):

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")

    r = Run.run_fasttrips(
        input_network_dir = INPUT_NETWORK,
        input_demand_dir  = INPUT_DEMAND,
        run_config        = os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_weights     = os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir        = OUTPUT_DIR,
        output_folder     = "test_dispers_%4.2d" % stop_process_count,
        pathfinding_type  = "stochastic",
        max_stop_process_count = stop_process_count,
        iters             = 1,
        dispersion        = 0.50 )

    assert passengers_arrived == r["passengers_arrived"]
