import os
import zipfile

from fasttrips import Run


def test_user_classes():

    EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR, "networks")
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, 'demand', "demand_reg")
    OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "output")

    scenario_dir = os.path.join(INPUT_NETWORKS, 'simple')
    scenario_file = os.path.join(INPUT_NETWORKS, 'simple.zip')
    with zipfile.ZipFile(scenario_file, 'w') as zipf:
        for root, dirs, files in os.walk(scenario_dir):
            for file in files:
                zipf.write(os.path.join(root, file), file)

    r = Run.run_fasttrips(
        input_network_dir   = scenario_file,
        input_demand_dir    = INPUT_DEMAND,
        run_config          = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights       = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        input_functions     = os.path.join(INPUT_DEMAND,"config_ft.py"),
        output_dir          = OUTPUT_DIR,
        output_folder       = "test_userclasses",
        pathfinding_type    = "stochastic",
        iters               = 1,
        dispersion          = 0.50,
        test_size           = 100,
        number_of_processes = 1)
    
    assert r["passengers_arrived"] > 0
    os.unlink(scenario_file)

