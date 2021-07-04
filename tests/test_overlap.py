import os,sys
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

# TEST PARAMETERS
OVERLAP_VARIABLES = ["None", "count", "distance", "time"]

@pytest.mark.parametrize("overlap_var", OVERLAP_VARIABLES)
@pytest.mark.parametrize("split_links", [False, True])

@pytest.mark.travis
def test_overlap(overlap_var, split_links ):

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = CONFIG_FILE,
        input_weights    = INPUT_WEIGHTS,
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_var-%s_split-%s" % (overlap_var, split_links),
        max_stop_process_count = 2,
        pf_iters         = 2,
        pathfinding_type = "stochastic",
        overlap_variable = overlap_var,
        overlap_split_transit = split_links,
        iters            = 1,
        dispersion       = 0.50,
        num_trips        = 5)

    assert r["passengers_arrived"] > 0

if __name__ == "__main__":
    for var in OVERLAP_VARIABLES:
        print("Running test_overlap.py with variable: %s, NO link split"  % (var))
        test_overlap(var,False)
        print("Running test_overlap.py with variable: %s, YES link split"  % (var))
        test_overlap(var,True)
