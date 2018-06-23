import os
import pytest
from fasttrips import Run

@pytest.mark.parametrize("assignment_type", ["stochastic","deterministic"])

def test_assignment_type(assignment_type):

    EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORK = os.path.join(EXAMPLES_DIR, "networks", 'simple')
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR, "output")

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        overlap_variable = "None",
        pf_iters         = 2,
        max_stop_process_count = 2,
        output_folder    = assignment_type,
        pathfinding_type = assignment_type,
        iters            = 1,
        dispersion       = 0.50,
        num_trips  = 5)

    assert r["passengers_arrived"] > 0

if __name__ == "__main__":
    for at in ["stochastic","deterministic"]:
        test_assignment_type(at)
