import os

import pytest

from fasttrips import Run


def test_feedback_no_cap_const():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_feedback_no_cap_const",
        pathfinding_type = "stochastic",
        capacity         = False,
        iters            = 3,
        dispersion       = 0.50)
    
    assert r["passengers_arrived"] > 0
    
def test_feedback_with_cap_const():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_feedback_with_cap_const",
        pathfinding_type = "stochastic",
        capacity         = True,
        iters            = 3,
        dispersion       = 0.50)
    
    assert r["passengers_arrived"] > 0