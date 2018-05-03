import os
import pytest
from fasttrips import Run

@pytest.mark.parametrize("feedback_iters", [3])
@pytest.mark.parametrize("capacity_const", [False, True])

def test_feedback(feedback_iters,capacity_const):

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_feedback_iters-%d_capConst-%s" % (feedback_iters,capacity_const),
        pathfinding_type = "stochastic",
        capacity         = capacity_const,
        iters            = feedback_iters,
        dispersion       = 0.50)

    assert r["passengers_arrived"] > 0
