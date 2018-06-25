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

# LIST OF RUN PARAMETERS
ASSIGNMENT_TYPES    = ["stochastic","deterministic"]

@pytest.mark.parametrize("assignment_type", ASSIGNMENT_TYPES)

def test_assignment_type(assignment_type):
    OUTPUT_FOLDER       = "assignment_type_%s" % (assignment_type)
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = CONFIG_FILE,
        input_weights    = INPUT_WEIGHTS,
        output_dir       = OUTPUT_DIR,
        output_folder    = OUTPUT_FOLDER,
        overlap_variable = "None",
        pf_iters         = 2,
        max_stop_process_count = 2,
        pathfinding_type = assignment_type,
        iters            = 1,
        dispersion       = 0.50,
        num_trips        = 5)

    assert r["passengers_arrived"] > 0

if __name__ == "__main__":
    for at in ["stochastic","deterministic"]:
        test_assignment_type(at)
