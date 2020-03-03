import os
import pytest
from fasttrips import Run


EXAMPLE_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples","Springfield")

# DIRECTORY LOCATIONS
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')
INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'vermont')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'simpson_zorn')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'C')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')
OUTPUT_FOLDER       = "test_convergence"

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_CONFIG, 'config_ft.txt')
INPUT_FUNCTIONS     = os.path.join(INPUT_CONFIG, 'config_ft.py')
INPUT_WEIGHTS       = os.path.join(INPUT_CONFIG, 'pathweight_ft.txt')


def test_convergence():
    '''
    This test uses the learning and convergence gap keywords in the config.
    '''

    Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = CONFIG_FILE,
        input_functions  = INPUT_FUNCTIONS,
        input_weights    = INPUT_WEIGHTS,
        output_dir       = OUTPUT_DIR,
        output_folder    = OUTPUT_FOLDER,
        capacity         = True,
        iters            = 10,
        dispersion       = 0.50,
        num_trips        = 10,
    )


if __name__ == '__main__':
    test_convergence()
