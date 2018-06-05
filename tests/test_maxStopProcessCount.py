import os
import pytest
from fasttrips import Run

@pytest.mark.parametrize("max_stop_process_n", [10, 50, 80])

def test_max_stop_process_count(max_stop_process_n):

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
        pf_iters         = 2,
        output_folder    = "test_dispers_%4.2d" % max_stop_process_n,
        pathfinding_type = "stochastic",
        max_stop_process_count = max_stop_process_n,
        iters            = 1,
        test_size        = 2,
        dispersion       = 0.50 )

    assert r["passengers_arrived"] > 0
