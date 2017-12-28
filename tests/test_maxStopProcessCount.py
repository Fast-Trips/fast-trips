import os
import zipfile

from fasttrips import Run


def test_max_stop_process_count():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"fasttrips","Examples")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"networks")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,'demand',"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")

    scenario_dir = os.path.join(INPUT_NETWORKS, 'simple')
    scenario_file = os.path.join(INPUT_NETWORKS, 'simple.zip')
    with zipfile.ZipFile(scenario_file, 'w') as zipf:
        for root, dirs, files in os.walk(scenario_dir):
            for file in files:
                zipf.write(os.path.join(root, file), file)

    for max_spc in [10, 50, 100]: 

        r = Run.run_fasttrips(
            input_network_dir= scenario_file,
            input_demand_dir = INPUT_DEMAND,
            run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
            input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
            output_dir       = OUTPUT_DIR,
            output_folder    = "test_dispers_%4.2d" % max_spc,
            pathfinding_type = "stochastic",
            max_stop_process_count = max_spc,
            iters            = 1,
            dispersion       = 0.50 )
        
        assert r["passengers_arrived"] > 0
        assert r["capacity_gap"]      < 0.001
        assert r["passengers_missed"] == 0

    os.unlink(scenario_file)