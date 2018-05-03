import os
import pytest
from fasttrips import Run

@pytest.mark.parametrize("ignore_PF_fares", [True])
@pytest.mark.parametrize("ignore_EN_fares", [False, True])

def test_fares(ignore_PF_fares,ignore_EN_fares):

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_ignore_fares_PF-%s_EN-%s" % (ignore_PF_fares,ignore_EN_fares),
        pathfinding_type = "stochastic",
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 2,
        transfer_fare_ignore_pathfinding = ignore_PF_fares,
        transfer_fare_ignore_pathenum    = ignore_EN_fares)

    assert r["passengers_arrived"] > 0
