import os
from fasttrips import Run

# DIRECTORY LOCATIONS
EXAMPLE_DIR         = os.path.abspath(os.path.dirname(__file__))

INPUT_NETWORK       = os.path.join(EXAMPLE_DIR, 'networks', 'vermont')
INPUT_DEMAND        = os.path.join(EXAMPLE_DIR, 'demand', 'general')
INPUT_CONFIG        = os.path.join(EXAMPLE_DIR, 'configs', 'A')
OUTPUT_DIR          = os.path.join(EXAMPLE_DIR, 'output')
OUTPUT_FOLDER       = "general_run"

# INPUT FILE LOCATIONS
CONFIG_FILE         = os.path.join(INPUT_CONFIG, 'config_ft.txt')
INPUT_WEIGHTS       = os.path.join(INPUT_CONFIG, 'pathweight_ft.txt')

print("Running Fast-Trips in %s" % (EXAMPLE_DIR.split(os.sep)[-1:]))

Run.run_fasttrips(
    input_network_dir= INPUT_NETWORK,
    input_demand_dir = INPUT_DEMAND,
    run_config       = CONFIG_FILE,
    input_weights    = INPUT_WEIGHTS,
    output_dir       = OUTPUT_DIR,
    output_folder    = OUTPUT_FOLDER,
    pathfinding_type = "stochastic",
    overlap_variable = "count",
    overlap_split_transit = True,
    iters            = 3,
    dispersion       = 0.50)
