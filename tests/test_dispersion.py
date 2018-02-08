import os
import pytest
from fasttrips import Run


@pytest.fixture(scope='module', params=[1.0, 0.7, 0.5, 0.4, 0.1])
def dispersion_rate(request):
    return request.param

@pytest.fixture(scope='module')
def passengers_arrived(dispersion_rate):
    arrived = {
        1.0: 723,
        0.7: 726,
        0.5: 726,
        0.4: 726,
        0.1: 726,
    }

    return arrived[dispersion_rate]

def test_dispersion(dispersion_rate, passengers_arrived):

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples",)

    INPUT_NETWORK  = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")

    full_output_dir = os.path.join(OUTPUT_DIR, "test_dispers_%4.2f" % dispersion_rate)
    if not os.path.exists(full_output_dir):
        os.mkdir(full_output_dir)

    r = Run.run_fasttrips(
        input_network_dir = INPUT_NETWORK,
        input_demand_dir  = INPUT_DEMAND,
        run_config        = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights     = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir        = OUTPUT_DIR,
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        output_folder    = "test_dispers_%4.2f" % dispersion_rate,
        pathfinding_type  = "stochastic",
        iters             = 1,
        dispersion        = dispersion_rate,
        test_size         = 100 )

    assert passengers_arrived == r["passengers_arrived"]
