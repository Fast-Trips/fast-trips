import os
import zipfile

from fasttrips import Run


def test_psrc():
    """
    Test to ensure that more complex network, PSRC, is working. Also a
    useful benchmark for Partridge loader compared to transitfeed.
    """
    EXAMPLES_DIR = os.path.join(os.getcwd(), "fasttrips", "Examples")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR, "networks")
    INPUT_DEMAND = os.path.join(EXAMPLES_DIR, "demand", "psrc_1_1")
    OUTPUT_DIR = os.path.join(EXAMPLES_DIR, "output")

    scenario_dir = os.path.join(INPUT_NETWORKS, 'psrc_1_1')
    scenario_file = os.path.join(INPUT_NETWORKS, 'psrc_1_1.zip')
    with zipfile.ZipFile(scenario_file, 'w') as zipf:
        for root, dirs, files in os.walk(scenario_dir):
            for file in files:
                zipf.write(os.path.join(root, file), file)

    Run.run_fasttrips(
        input_network_dir=scenario_file,
        input_demand_dir=INPUT_DEMAND,
        run_config=os.path.join(INPUT_DEMAND, "config_ft.txt"),
        input_functions=os.path.join(INPUT_DEMAND, 'config_ft.py'),
        input_weights=os.path.join(INPUT_DEMAND, "pathweight_ft.txt"),
        output_dir=OUTPUT_DIR,
        output_folder="test_psrc",
        pathfinding_type="stochastic",
        capacity=True,
        iters=1,
        OVERLAP = "None",
        dispersion=1.0
    )


    os.unlink(scenario_file)

if __name__ == '__main__':
    test_psrc()
