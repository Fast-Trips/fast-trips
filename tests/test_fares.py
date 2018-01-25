import os

from fasttrips import Run


def test_fares_nopf():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"fasttrips","Examples")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"networks")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    scenario_dir = os.path.join(INPUT_NETWORKS, 'simple')

    r = Run.run_fasttrips(
        input_network_dir= scenario_dir,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_fares_nopf",
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100,
        transfer_fare_ignore_pathfinding = True )
    
    assert r["passengers_arrived"] > 0


def test_fares_nopf_nopf():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"fasttrips","Examples")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"networks")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,'demand', "demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    scenario_dir = os.path.join(INPUT_NETWORKS, 'simple')

    r = Run.run_fasttrips(
        input_network_dir= scenario_dir,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_fares_nopf_nope",
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100,
        transfer_fare_ignore_pathfinding = True,
        transfer_fare_ignore_pathenum    = True )
    
    assert r["passengers_arrived"] > 0

