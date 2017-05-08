import os

import pytest

import fasttrips
from fasttrips import Run



def test_dispersion():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    for d in [1.0, 0.7, 0.5, 0.4, 0.1]: 
        
        full_output_dir = os.path.join(OUTPUT_DIR,"test_dispers_%4.2f" % d)
        if not os.path.exists(full_output_dir): os.mkdir(full_output_dir)
        print "STARTING RUN"
        r = Run.run_fasttrips(
            input_network_dir= INPUT_NETWORKS,
            input_demand_dir = INPUT_DEMAND,
            run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
            input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
            output_dir       = OUTPUT_DIR,
            output_folder    = "test_dispers_%4.2f" % d,
            pathfinding_type = "stochastic",
            iters            = 1,
            dispersion       = d,
            test_size        = 100 )
        
        print "FINISHED RUN"
        assert r["passengers_arrived"] > 0
        assert r["capacity_gap"]      < 0.001
        assert r["passengers_missed"] == 0
