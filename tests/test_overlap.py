import os

import pytest

import fasttrips
from fasttrips import Run

def test_overlap_none():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")
    OVERLAP_TYPE   = "None"
    
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_%s" % (OVERLAP_TYPE),
        overlap_variable = OVERLAP_TYPE,
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100 )
    
    assert r["passengers_arrived"] > 0

def test_overlap_count():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")
    OVERLAP_TYPE   = "count"
    
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_%s" % (OVERLAP_TYPE),
        overlap_variable = OVERLAP_TYPE,
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100 )
    
    assert r["passengers_arrived"] > 0
    
    
def test_overlap_distance():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")
    OVERLAP_TYPE   = "distance"
    
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_%s" % (OVERLAP_TYPE),
        overlap_variable = OVERLAP_TYPE,
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100 )
    
    assert r["passengers_arrived"] > 0
    
    
def test_overlap_time():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")
    OVERLAP_TYPE   = "time"
    
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_%s" % (OVERLAP_TYPE),
        overlap_variable = OVERLAP_TYPE,
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100 )
    
    assert r["passengers_arrived"] > 0

def test_overlap_count_with_split():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")
    OVERLAP_TYPE   = "count"
    
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_%s_wSplit" % (OVERLAP_TYPE),
        overlap_variable = OVERLAP_TYPE,
        pathfinding_type = "stochastic",
        overlap_split_transit = True,
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100 )
    
    assert r["passengers_arrived"] > 0
    
    
def test_overlap_distance_with_split():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")
    OVERLAP_TYPE   = "distance"
    
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_%s_wSplit" % (OVERLAP_TYPE),
        overlap_variable = OVERLAP_TYPE,
        pathfinding_type = "stochastic",
        overlap_split_transit = True,
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100 )
    
    assert r["passengers_arrived"] > 0
    
    
def test_overlap_time_with_split():

    EXAMPLES_DIR   = os.path.join(os.getcwd(),"Examples","test_scenario")

    INPUT_NETWORKS = os.path.join(EXAMPLES_DIR,"network")
    INPUT_DEMAND   = os.path.join(EXAMPLES_DIR,"demand_reg")
    OUTPUT_DIR     = os.path.join(EXAMPLES_DIR,"output")
    OVERLAP_TYPE   = "time"
    
    r = Run.run_fasttrips(
        input_network_dir= INPUT_NETWORKS,
        input_demand_dir = INPUT_DEMAND,
        run_config       = os.path.join(INPUT_DEMAND,"config_ft.txt"),
        input_weights    = os.path.join(INPUT_DEMAND,"pathweight_ft.txt"),
        output_dir       = OUTPUT_DIR,
        output_folder    = "test_overlap_%s_wSplit" % (OVERLAP_TYPE),
        overlap_variable = OVERLAP_TYPE,
        overlap_split_transit = True,
        pathfinding_type = "stochastic",
        iters            = 1,
        dispersion       = 0.50,
        test_size        = 100 )
    
    assert r["passengers_arrived"] > 0
    