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
ignore_PF_fares_options = [True]
ignore_EN_fares_options = [False, True]

@pytest.mark.parametrize("ignore_PF_fares", ignore_PF_fares_options)
@pytest.mark.parametrize("ignore_EN_fares", ignore_EN_fares_options)

@pytest.mark.travis
def test_fares(ignore_PF_fares,ignore_EN_fares):

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORK,
        input_demand_dir = INPUT_DEMAND,
        run_config       = CONFIG_FILE,
        input_weights    = INPUT_WEIGHTS,
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_ignore_fares_PF-%s_EN-%s" % (ignore_PF_fares,ignore_EN_fares),
        pathfinding_type = "stochastic",
        max_stop_process_count = 2,
        pf_iters         = 2,
        overlap_variable = "None",
        iters            = 1,
        dispersion       = 0.50,
        num_trips        = 5,
        transfer_fare_ignore_pathfinding = ignore_PF_fares,
        transfer_fare_ignore_pathenum    = ignore_EN_fares)

    assert r["passengers_arrived"] > 0

if __name__ == '__main__':
    import itertools
    for ignore_PF_fares,ignore_EN_fares in list(itertools.product(ignore_PF_fares_options, ignore_EN_fares_options)):
        print("running %s %s" % (ignore_PF_fares,ignore_EN_fares))
        test_fares(ignore_PF_fares,ignore_EN_fares)
