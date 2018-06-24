import os
import pytest
from fasttrips import Run

EXAMPLE_DIR    = os.path.join(os.getcwd(), 'fasttrips', 'Examples', 'Seattle_Region')

# DIRECTORY LOCATIONS
INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'psrc_1_1')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'psrc_1_1')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'base')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_CONFIG, 'config_ft.txt')
INPUT_FUNCTIONS     = os.path.join(INPUT_CONFIG, 'config_ft.py')
INPUT_WEIGHTS       = os.path.join(INPUT_CONFIG, 'pathweight_ft.txt')

@pytest.mark.travis
def test_psrc():
    """
    Test to ensure that more complex network, PSRC, is working. Also a
    useful benchmark for Partridge loader compared to transitfeed.
    """

    Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = CONFIG_FILE,
        input_weights    = INPUT_WEIGHTS,
        input_functions  = INPUT_FUNCTIONS,
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_psrc",
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        pathfinding_type = "stochastic",
        capacity         = True,
        iters            = 1,
        OVERLAP          = "None",
        dispersion       = 1.0,
        num_trips        = 5,
    )


if __name__ == '__main__':
    test_psrc()
