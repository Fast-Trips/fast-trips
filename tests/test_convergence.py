import os
import pytest

import zipfile

from fasttrips import Run

"test_scenario"

EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples", )
INPUT_NETWORKS = os.path.join(EXAMPLES_DIR, "networks")
INPUT_DEMAND = os.path.join(EXAMPLES_DIR, "test_scenario", "demand_twopaths")
OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "test_convergence")

def test_run_capacity_test():
    scenario_dir = os.path.join(INPUT_NETWORKS, 'simple')
    scenario_file = os.path.join(INPUT_NETWORKS, 'simple.zip')
    with zipfile.ZipFile(scenario_file, 'w') as zipf:
        for root, dirs, files in os.walk(scenario_dir):
            for file in files:
                zipf.write(os.path.join(root, file), file)

    performance = (OUTPUT_DIR, Run.run_fasttrips(
        input_network_dir=scenario_file,
        input_demand_dir=INPUT_DEMAND,
        run_config=os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_functions=os.path.join(INPUT_DEMAND, 'config_ft.py'),
        input_weights=os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir=OUTPUT_DIR,
        output_folder="output",
        pathfinding_type="stochastic",
        capacity=True,
        iters=4,
        dispersion=0.50))


    os.unlink(scenario_file)

    return performance
