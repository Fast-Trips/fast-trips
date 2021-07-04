from __future__ import print_function
from __future__ import division
from past.builtins import execfile
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import str
from builtins import range
from builtins import object

__copyright__ = "Copyright 2015-2017 Contributing Entities"
__license__   = """
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""
import configparser
import datetime
import math
import multiprocessing
import os
import queue
import sys
import traceback

import numpy as np
import pandas as pd

import _fasttrips
from .Error import ConfigurationError
from .Logger import FastTripsLogger, setupLogging
from .Passenger import Passenger
from .PathSet import PathSet
from .Performance import Performance
from .Trip import Trip
from .Util import Util


class Assignment(object):
    """
    Assignment class.  Documentation forthcoming.

    """
    #: Configuration file for fasttrips
    CONFIGURATION_FILE              = None
    #: Configuration functions
    CONFIGURATION_FUNCTIONS_FILE    = None

    #: Output copy of the configuration file in case anything got overridden
    #: (Hmm naming conventions are a bit awkward here)
    CONFIGURATION_OUTPUT_FILE       = 'ft_output_config.txt'

    #: Configuration: Input network directory
    INPUT_NETWORK_ARCHIVE               = None
    #: Configuration: Input demand directory
    INPUT_DEMAND_DIR                = None
    #: Configuration: Pathweight parameters
    INPUT_WEIGHTS                   = None
    #: Configuration: Run Configuration
    OUTPUT_DIR                      = None

    #: Configuration: Maximum number of iterations to remove capacity violations. When
    #: the transit system is not crowded or when capacity constraint is
    #: relaxed the model will terminate after the first iteration
    MAX_ITERATIONS                  = None
    MAX_PF_ITERATIONS               = 10
    CONVERGENCE_GAP                 = None

    NETWORK_BUILD_DATE              = datetime.datetime.today()
    NETWORK_BUILD_DATE_START_TIME   = datetime.datetime.combine(NETWORK_BUILD_DATE, datetime.time())
    #: Find paths deterministically, using shortest path search based on travel time.
    PATHFINDING_TYPE_DETERMINISTIC  = 'deterministic'
    #: Find paths stochastically using trip-based hyperpath
    PATHFINDING_TYPE_STOCHASTIC     = 'stochastic'
    #: Don't find paths; read :py:attr:`Passenger.PF_PATHS_CSV` and :py:attr:`Passenger.PF_LINKS_CSV`.
    PATHFINDING_TYPE_READ_FILE      = 'file'
    #: Configuration: Pathfinding Type.  Should be one of `Deterministic`, `Stochastic` or `File`
    PATHFINDING_TYPE                = None

    #: Configuration: Do simulation? It should be True for iterative assignment. In a one shot
    #: assignment with simulation flag off, the passengers are assigned to
    #: paths but are not loaded to the network.  Boolean.
    SIMULATION                      = None

    #: Configuration: Passenger trajectory output flag. Passengers' path and time will be
    #: reported if this flag is on. Note that the simulation flag should be on for
    #: passengers' time.  Boolean.
    OUTPUT_PASSENGER_TRAJECTORIES   = None

    #: Configuration: If true, outputs pathset every simulation iteration.  If false,
    #: outputs pathset every path-finding iteration.
    OUTPUT_PATHSET_PER_SIM_ITER     = None

    #: Configuration: Path time-window. This is the time in which the paths are generated.
    #: E.g. with a typical 30 min window, any path within 30 min of the
    #: departure time will be checked.  A :py:class:`datetime.timedelta` instance.
    TIME_WINDOW                     = None

    #: Configuration: Create skims flag. This is specific to the travel demand models
    #: (not working in this version). Boolean.
    CREATE_SKIMS                    = None

    #: Configuration: Beginning of the time period for which the skim is required.
    #:  (specify as 'HH:MM'). A :py:class:`datetime.datetime` instance.
    SKIM_START_TIME                 = None

    #: Configuration: End of the time period for which the skim is required
    #: (specify as 'HH:MM'). A :py:class:`datetime.datetime` instance.
    SKIM_END_TIME                   = None

    #: Route choice configuration: Max number of paths in a pathset.
    #: Used in conjuntion with :py:attr:`Assignment.MIN_PATH_PROBABILITY`
    MAX_NUM_PATHS                   = None

    #: Route choice configuration: Minimum path probability for the path to be used.
    #: Used in conjucntion with :py:attr:`Assignment.MAX_NUM_PATHS`, so it only
    #: kicks in if that is specified AND we hit it, then we start dropping using
    #: this threshhold.
    MIN_PATH_PROBABILITY            = None

    #: Route choice configuration: Dispersion parameter in the logit function.
    #: Higher values result in less stochasticity. Must be nonnegative.
    #: If unknown use a value between 0.5 and 1. Float.
    STOCH_DISPERSION                = None

    #: Stop labeling configuration: Multiplies the utilities by this factor
    #: so that there are not negative costs labels which can result in
    #: lengthy and ineffective path-finding. Must be positive; should be greater
    #: than 1.0.  Double.
    UTILS_CONVERSION                = None

    #: In path-finding, suppress trying to adjust fares using transfer fare rules.
    #: This is for performance testing.
    TRANSFER_FARE_IGNORE_PATHFINDING = None
    #: In path-enumeration, suppress trying to adjust fares using transfer fare rules.
    #: This is for performance testing.
    TRANSFER_FARE_IGNORE_PATHENUM    = None

    #: Route choice configuration: How many times max times should we process a stop
    #: during labeling?  Use -1 to specify no max.  Int.
    #: Setting this to a positive value may increase runtime but may decrease
    #: pathset quality. (Todo: test/quantify this.)
    STOCH_MAX_STOP_PROCESS_COUNT    = None

    #: Route choice configuration: How many stochastic paths will we generate
    #: (not necessarily unique) to define a path choice set?  Int.
    STOCH_PATHSET_SIZE              = None

    #: Route choice configuration: Use vehicle capacity constraints. Boolean.
    CAPACITY_CONSTRAINT             = None

    #: Debug mode: only run trace passengers
    DEBUG_TRACE_ONLY                = False

    #: Debug mode: only run this number of trips, -1 to run all. Int.
    DEBUG_NUM_TRIPS                 = -1

    #: Debug: include debug columns in output
    DEBUG_OUTPUT_COLUMNS            = False

    #: Fare zone symmetry. If True, will assume fare zone symmetry.  That is, if fare_id X is
    # configured from origin zone A to destination zone B, and there is no fare configured
    # from zone B to zone A, we'll assume that fare_id X also applies.
    FARE_ZONE_SYMMETRY              = False

    #: Skip these passengers
    SKIP_PERSON_IDS                 = None

    #: Trace these persons/person trips (a list of tuples)
    TRACE_IDS                       = []

    #: Prepend the route id to the trip id?  This is for readability in debugging, since
    #: route IDs are typically more readable and trip ids are inscrutable
    PREPEND_ROUTE_ID_TO_TRIP_ID     = False

    #: Number of processes to use for path finding (via :py:mod:`multiprocessing`)
    #: Set to 1 to run everything in this process
    #: Set to less than 1 to use the result of :py:func:`multiprocessing.cpu_count`
    #: Set to positive integer greater than 1 to set a fixed number of processes
    NUMBER_OF_PROCESSES             = None

    #: Extra time so passengers don't get bumped (?). A :py:class:`datetime.timedelta` instance.
    BUMP_BUFFER                     = None

    #: This is the only simulation state that exists across iterations
    #: It's a dictionary of (trip_id, stop_id) -> earliest time a bumped passenger started waiting
    bump_wait                       = {}
    bump_wait_df                    = None

    #: Simulation: bump one stop at a time (slower, more accurate)
    #:
    #: When addressing capacity constraints in simulation, we look at all the (trip, stop)-pairs
    #: where the boards are not allowed since vehicle is over capacity.  The faster way to address
    #: this is to bump all of those passengers, which means we call the assigned path bad and try
    #: to reassign.
    #:
    #: However, this could over-bump passengers, because if a passenger rides multiple
    #: crowded vehicles, then bumping her frees up space on other vehicles and so some other bumping
    #: may not be necessary.  Thus, the more accurate (but slower) method is to bump passengers from
    #: each (trip,stop) at a time, in order of the full vehicle arrival time, and then recalculate
    #: loads, and iterate until we have no capacity issues.  Boolean.
    BUMP_ONE_AT_A_TIME              = None

    #: MSA the results that affect the next iteration to avoid oscillation: boards, alights, overcap onboard at stops
    MSA_RESULTS                     = False

    #: Are we finding paths for everyone right now?  Or just un-arrived folks?
    PATHFINDING_EVERYONE            = True

    #: How many Simulation Iterations should we do before going back to path-finding?
    MAX_SIMULATION_ITERS            = 10

    #: Column names for simulation
    SIM_COL_PAX_BOARD_TIME          = 'board_time'       #: Board time on the transit vehicle
    SIM_COL_PAX_ALIGHT_TIME         = 'alight_time'      #: Alight time from the transit vehicle

    SIM_COL_PAX_ALIGHT_DELAY_MIN    = 'alight_delay_min' #: Delay in alight_time from original pathfinding understanding of alight time
    SIM_COL_PAX_A_TIME              = 'new_A_time'       #: Time of arrival at A
    SIM_COL_PAX_B_TIME              = 'new_B_time'       #: Time of arrival at B
    SIM_COL_PAX_LINK_TIME           = 'new_linktime'     #: Link time (SIM_COL_PAX_B_TIME - SIM_COL_PAX_A_TIME)
    SIM_COL_PAX_WAIT_TIME           = 'new_waittime'     #: Wait time
    SIM_COL_PAX_MISSED_XFER         = 'missed_xfer'      #: Is this link a missed transfer

    SIM_COL_PAX_OVERCAP             = Trip.SIM_COL_VEH_OVERCAP      #:
    SIM_COL_PAX_OVERCAP_FRAC        = Trip.SIM_COL_VEH_OVERCAP_FRAC #: If board at an overcap stop, fraction of boards that are overcap
    SIM_COL_PAX_BUMP_ITER           = 'bump_iter'
    SIM_COL_PAX_BOARD_STATE         = 'board_state'      #: NaN if not relevent, 1 if lucky enough to board at an at- or over-capacity stop, 0 if bumped.  Set by :py:meth:`Assignment.flag_bump_overcap_passengers`
    SIM_COL_PAX_DISTANCE            = "distance"         #: Link distance
    SIM_COL_PAX_FARE                = "fare"             #: Link fare in currency
    SIM_COL_PAX_FARE_PERIOD         = "fare_period"      #: Fare period id
    SIM_COL_PAX_FREE_TRANSFER       = "free_transfer"    #: Free transfer?  NaN, 0.0 or 1.0, only free based on `fare_attributes_ft.txt`
    SIM_COL_PAX_COST                = 'sim_cost'         #: Link cost. (Cannot be `cost` because it collides with TAZ.DRIVE_ACCESS_COLUMN_COST)
    SIM_COL_PAX_LNPS                = 'ln_PS'            #: log(PathSize)
    SIM_COL_PAX_PROBABILITY         = 'probability'      #: Probability of this path
    SIM_COL_PAX_LOGSUM              = 'logsum'           #: Logsum of all paths

    #: Is this link/path a missed transfer?
    #: Set in both pathset links and pathset paths, this is a 1 or 0
    SIM_COL_MISSED_XFER             = 'missed_xfer'

    #: Values for :py:attr:`Assignment.SIM_COL_PAX_BOARD_STATE` column
    BOARD_STATE_CATEGORICAL    = [ \
        "board_easy",       #: path chosen and no capacity problems
        "boarded",          #: path chosen and lucky enough to board an at-capacity or over-capacity vehicle
        "bumped",           #: path chosen but bumped due to capacity problems
        "bumped_othertrip", #: path invalidated due to being bumped on another link of this person's trip
        "bumped_unchosen"]  #: path invalidated before ever chosen due to capacity problems

    #: Chosen status for path
    SIM_COL_PAX_CHOSEN              = 'chosen'
    #: categories for SIM_COL_PAX_CHOSEN
    CHOSEN_NOT_CHOSEN_YET           = "unchosen"
    CHOSEN_REJECTED                 = "rejected"
    #: These will be ordered, so to select chosen, choose those > CHOSEN_NOT_CHOSEN_YET
    CHOSEN_CATEGORIES               = [CHOSEN_REJECTED,CHOSEN_NOT_CHOSEN_YET]

    def __init__(self):
        """
        This does nothing.  Assignment methods are static methods for now.
        """
        pass

    @staticmethod
    def read_functions(func_file):
        """
        Read the functions from :py:attr:`Assignment.CONFIGURATION_FUNCTIONS_FILE
        """
        # Functions are defined in here -- read this and eval it
        if func_file and os.path.exists(func_file):
            my_globals = {}
            FastTripsLogger.info("Reading %s" % func_file)
            execfile(func_file, my_globals, PathSet.CONFIGURED_FUNCTIONS)
            FastTripsLogger.info("PathSet.CONFIGURED_FUNCTIONS = %s" % str(PathSet.CONFIGURED_FUNCTIONS))

    @staticmethod
    def read_configuration(config_fullpath):
        """
        Read the configuration parameters from :py:attr:`Assignment.CONFIGURATION_FILE`
        """
        pd.set_option('display.width',      1000)
        # pd.set_option('display.height',   1000)
        pd.set_option('display.max_rows',   1000)
        pd.set_option('display.max_columns', 100)

        parser = configparser.RawConfigParser(
            defaults={'max_iterations'                  :1,
                      'max_pf_iterations'               :1,
                      'network_build_date'              : datetime.date.today().strftime("%m/%d/%Y"),
                      'simulation'                      :'True',
                      'learning_convergence'            :'False',
                      'learning_rate'                   : 0.01,
                      'convergence_gap'                 : 0.001,
                      'output_pathset_per_sim_iter'     :'False',
                      'output_passenger_trajectories'   :'True',
                      'create_skims'                    :'False',
                      'skim_start_time'                 :'5:00',
                      'skim_end_time'                   :'10:00',
                      'capacity_constraint'             :'False',
                      'skip_person_ids'                 :'None',
                      'trace_ids'                       :[],
                      'debug_trace_only'                :'False',
                      'debug_num_trips'                 :-1,
                      'debug_output_columns'            :'False',
                      'fare_zone_symmetry'              :'False',
                      'prepend_route_id_to_trip_id'     :'False',
                      'number_of_processes'             :0,
                      'bump_buffer'                     :5,
                      'bump_one_at_a_time'              :'False',

                      # pathfinding
                      'max_num_paths'                    :-1,
                      'min_path_probability'             :0.005,
                      'min_transfer_penalty'             :0.1,
                      'overlap_chunk_size'               :500,
                      'overlap_scale_parameter'          :1.0,
                      'overlap_split_transit'            :'False',
                      'overlap_variable'                 :'count',
                      'pathfinding_type'                 :Assignment.PATHFINDING_TYPE_STOCHASTIC,
                      'pathweights_fixed_width'          :'False',
                      'utils_conversion_factor'         :1.0,
                      'stochastic_dispersion'            :1.0,
                      'stochastic_max_stop_process_count':20,
                      'stochastic_pathset_size'          :1000,
                      'time_window'                      :30,
                      'transfer_fare_ignore_pathfinding' :'False',
                      'transfer_fare_ignore_pathenum'    :'False',
                      'user_class_function'              :'generic_user_class',
                      'arrive_late_allowed_min'          : 0,
                      'depart_early_allowed_min'         : 0,
                     })

        # Read configuration from specified configuration directory
        FastTripsLogger.info("Reading configuration file %s" % config_fullpath)
        parser.read(config_fullpath)

        Assignment.MAX_ITERATIONS                = parser.getint    ('fasttrips','max_iterations')
        Assignment.MAX_PF_ITERATIONS                = parser.getint ('fasttrips','max_pf_iterations')
        Assignment.NETWORK_BUILD_DATE            = datetime.datetime.strptime(
                                                    parser.get('fasttrips', 'network_build_date'), '%m/%d/%Y').date()
        Assignment.NETWORK_BUILD_DATE_START_TIME = datetime.datetime.combine(Assignment.NETWORK_BUILD_DATE, datetime.time())
        Assignment.SIMULATION                    = parser.getboolean('fasttrips','simulation')
        PathSet.LEARN_ROUTES                     = parser.getboolean('fasttrips', 'learning_convergence')
        PathSet.LEARN_ROUTES_RATE                = parser.getfloat('fasttrips', 'learning_rate')
        Assignment.CONVERGENCE_GAP               = parser.getfloat('fasttrips', 'convergence_gap')
        Assignment.OUTPUT_PASSENGER_TRAJECTORIES = parser.getboolean('fasttrips','output_passenger_trajectories')
        Assignment.OUTPUT_PATHSET_PER_SIM_ITER   = parser.getboolean('fasttrips','output_pathset_per_sim_iter')
        Assignment.CREATE_SKIMS                  = parser.getboolean('fasttrips','create_skims')
        Assignment.SKIM_START_TIME = datetime.datetime.strptime(
                                                   parser.get       ('fasttrips','skim_start_time'),'%H:%M')
        Assignment.SKIM_END_TIME   = datetime.datetime.strptime(
                                                   parser.get       ('fasttrips','skim_end_time'),'%H:%M')
        Assignment.CAPACITY_CONSTRAINT           = parser.getboolean('fasttrips','capacity_constraint')
        Assignment.SKIP_PERSON_IDS               = eval(parser.get       ('fasttrips','skip_person_ids'))
        try:
            Assignment.TRACE_IDS                     = eval(parser.get       ('fasttrips','trace_ids'))
        except:
            e = "Must have a TRACE_IDS line in config_ft.txt; even if it is trace_ids = []"
            print(e)
            FastTripsLogger.error(e)
            raise
        Assignment.DEBUG_TRACE_ONLY              = parser.getboolean('fasttrips','debug_trace_only')
        Assignment.DEBUG_NUM_TRIPS               = parser.getint    ('fasttrips','debug_num_trips')
        Assignment.DEBUG_OUTPUT_COLUMNS          = parser.getboolean('fasttrips','debug_output_columns')
        Assignment.FARE_ZONE_SYMMETRY            = parser.getboolean('fasttrips','fare_zone_symmetry')
        Assignment.PREPEND_ROUTE_ID_TO_TRIP_ID   = parser.getboolean('fasttrips','prepend_route_id_to_trip_id')
        Assignment.NUMBER_OF_PROCESSES           = parser.getint    ('fasttrips','number_of_processes')
        Assignment.BUMP_BUFFER = datetime.timedelta(
                                         minutes = parser.getfloat  ('fasttrips','bump_buffer'))
        Assignment.BUMP_ONE_AT_A_TIME            = parser.getboolean('fasttrips','bump_one_at_a_time')

        # pathfinding
        Assignment.MAX_NUM_PATHS                 = parser.getint    ('pathfinding','max_num_paths')
        Assignment.MIN_PATH_PROBABILITY          = parser.getfloat  ('pathfinding','min_path_probability')
        PathSet.MIN_TRANSFER_PENALTY             = parser.getfloat  ('pathfinding','min_transfer_penalty')
        PathSet.OVERLAP_CHUNK_SIZE               = parser.getint    ('pathfinding','overlap_chunk_size')
        PathSet.OVERLAP_SCALE_PARAMETER          = parser.getfloat  ('pathfinding','overlap_scale_parameter')
        PathSet.OVERLAP_SPLIT_TRANSIT            = parser.getboolean('pathfinding','overlap_split_transit')
        PathSet.OVERLAP_VARIABLE                 = parser.get       ('pathfinding','overlap_variable')
        Assignment.PATHFINDING_TYPE              = parser.get       ('pathfinding','pathfinding_type')
        PathSet.WEIGHTS_FIXED_WIDTH              = parser.getboolean('pathfinding','pathweights_fixed_width')
        Assignment.STOCH_DISPERSION              = parser.getfloat  ('pathfinding','stochastic_dispersion')
        Assignment.UTILS_CONVERSION              = parser.getfloat  ('pathfinding','utils_conversion_factor')
        Assignment.STOCH_MAX_STOP_PROCESS_COUNT  = parser.getint    ('pathfinding','stochastic_max_stop_process_count')
        Assignment.STOCH_PATHSET_SIZE            = parser.getint    ('pathfinding','stochastic_pathset_size')
        Assignment.TIME_WINDOW = datetime.timedelta(
                                         minutes = parser.getfloat  ('pathfinding','time_window'))

        Assignment.TRANSFER_FARE_IGNORE_PATHFINDING = parser.getboolean('pathfinding','transfer_fare_ignore_pathfinding')
        Assignment.TRANSFER_FARE_IGNORE_PATHENUM    = parser.getboolean('pathfinding','transfer_fare_ignore_pathenum')
        PathSet.USER_CLASS_FUNCTION                 = parser.get       ('pathfinding','user_class_function')
        PathSet.DEPART_EARLY_ALLOWED_MIN            = datetime.timedelta(
                                            minutes = parser.getfloat('pathfinding', 'depart_early_allowed_min'))
        PathSet.ARRIVE_LATE_ALLOWED_MIN             = datetime.timedelta(
                                            minutes = parser.getfloat  ('pathfinding','arrive_late_allowed_min'))

        if Assignment.PATHFINDING_TYPE not in [Assignment.PATHFINDING_TYPE_STOCHASTIC, \
                                               Assignment.PATHFINDING_TYPE_DETERMINISTIC, \
                                               Assignment.PATHFINDING_TYPE_READ_FILE]:
            msg = "pathfinding type [%s] not available. Expected values: %s" % (Assignment.PATHFINDING_TYPE, str([Assignment.PATHFINDING_TYPE_STOCHASTIC, Assignment.PATHFINDING_TYPE_DETERMINISTIC,  Assignment.PATHFINDING_TYPE_READ_FILE]))
            FastTripsLogger.fatal(msg)
            raise ConfigurationError(config_fullpath, msg)

        if PathSet.OVERLAP_VARIABLE not in PathSet.OVERLAP_VARIABLE_OPTIONS:
            msg = "pathfinding.overlap_variable [%s] not defined. Expected values: %s" % (PathSet.OVERLAP_VARIABLE, str(PathSet.OVERLAP_VARIABLE_OPTIONS))
            FastTripsLogger.fatal(msg)
            raise ConfigurationError(config_fullpath, msg)
        if PathSet.USER_CLASS_FUNCTION not in PathSet.CONFIGURED_FUNCTIONS:
            msg = "User class function [%s] not defined.  Please check your function file [%s]" % (PathSet.USER_CLASS_FUNCTION, Assignment.CONFIGURATION_FUNCTIONS_FILE)
            FastTripsLogger.fatal(msg)
            raise ConfigurationError(config_fullpath, msg)


    @staticmethod
    def read_weights(weights_file = INPUT_WEIGHTS):
        """
        Read the weights from :py:attr:`Assignment.INPUT_WEIGHTS
        """
        if not os.path.exists(weights_file):
            FastTripsLogger.fatal("No path weights file %s" % weights_file)
            sys.exit(2)

        if PathSet.WEIGHTS_FIXED_WIDTH:
            weights = pd.read_fwf(weights_file)
            weights[PathSet.WEIGHTS_COLUMN_PURPOSE] = weights[PathSet.WEIGHTS_COLUMN_PURPOSE].astype(str)
        else:
            print (PathSet.WEIGHTS_COLUMN_PURPOSE)
            ##LMZ
            weights = pd.read_csv(weights_file, dtype={PathSet.WEIGHTS_COLUMN_PURPOSE:'S'}, skipinitialspace=True)

        PathSet.WEIGHTS_DF = Assignment.process_weight_qualifiers(weights)

        FastTripsLogger.debug("Weights =\n%s" % str(PathSet.WEIGHTS_DF))
        FastTripsLogger.debug("Weight types = \n%s" % str(PathSet.WEIGHTS_DF.dtypes))


    @staticmethod
    def process_weight_qualifiers(weights):
        """
        Qualifiers are used to change the default behavior of weight_names. Qualifiers
        are added by adding a period (.) after the weight_name and specifying the
        qualifier name. Qualifier attributes are specified after a second dot.

        For example: depart_early_cost_min.logistic.growth_rate
        depart_early_cost_min is being qualified as a logistic penalty instead of the default
        behavior. growth_rate is an attribute of the logistic qualifier.
        :param weights: vertically oriented qualifiers
        :return: pivoted weights table with the qualifiers normalized horizontally.
        """

        growth_type = weights[weights[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.count('\.') == 1].copy()
        qualifiers = weights[weights[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.count('\.') == 2].copy()
        weights = weights[~weights[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.contains('\.')].copy()
        weights[PathSet.WEIGHTS_GROWTH_TYPE] = PathSet.CONSTANT_GROWTH_MODEL

        # if only 'linear' this process is done and can return
        if growth_type.shape[0] == 0:
            return weights

        growth_type[PathSet.WEIGHTS_GROWTH_TYPE] = growth_type[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.extract('((?<=\.)\w+)', expand=False)
        growth_type[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME] = growth_type[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME].str.extract('(\w+(?=\.))', expand=False)
        weights = pd.concat([weights, growth_type])

        if (~weights[PathSet.WEIGHTS_GROWTH_TYPE].isin(PathSet.PENALTY_GROWTH_MODELS)).any():
            FastTripsLogger.fatal("Invalid qualifier type specified.")
            raise KeyError('Invalid qualifier type specified.')

        # if only 'constant' and 'exponential' this process is done and can return.
        if qualifiers.shape[0] == 0:
            return weights

        qualifiers = qualifiers.rename(columns={PathSet.WEIGHTS_COLUMN_WEIGHT_NAME: 'qualifier'})
        qualifiers[PathSet.WEIGHTS_COLUMN_WEIGHT_NAME] = qualifiers['qualifier'].str.extract('(^\w+)', expand=False)
        qualifiers['variable'] = qualifiers['qualifier'].str.extract('(\w+$)', expand=False)

        qualifier_values = qualifiers.pivot(columns='variable', values=PathSet.WEIGHTS_COLUMN_WEIGHT_VALUE)
        qualifier_columns = qualifier_values.columns.values

        qualifiers = pd.concat([qualifiers, qualifier_values], axis=1)

        merge_cols = [
            PathSet.WEIGHTS_COLUMN_USER_CLASS, PathSet.WEIGHTS_COLUMN_PURPOSE,
            PathSet.WEIGHTS_COLUMN_DEMAND_MODE_TYPE, PathSet.WEIGHTS_COLUMN_DEMAND_MODE,
            PathSet.WEIGHTS_COLUMN_SUPPLY_MODE, PathSet.WEIGHTS_COLUMN_WEIGHT_NAME,
        ]

        qualifiers = qualifiers.groupby(merge_cols)[qualifier_columns].max().reset_index()

        weights_df = pd.merge(weights, qualifiers, on=merge_cols, how='left')

        #Check for required logarithmic attributes and that they are all non-zero and positive
        if len(weights_df[weights_df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.LOGARITHMIC_GROWTH_MODEL]) > 0:
            assert(PathSet.WEIGHTS_GROWTH_LOG_BASE in weights_df)
            assert(
                len(weights_df[
                    (weights_df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.LOGARITHMIC_GROWTH_MODEL) &
                    (weights_df[PathSet.WEIGHTS_GROWTH_LOG_BASE] <= 0)
                ]) == 0)

        if len(weights_df[weights_df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.LOGISTIC_GROWTH_MODEL]) > 0:
            assert(PathSet.WEIGHTS_GROWTH_LOGISTIC_MID in weights_df)
            assert (PathSet.WEIGHTS_GROWTH_LOGISTIC_MAX in weights_df)
            assert(
                len(weights_df[
                    (weights_df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.LOGISTIC_GROWTH_MODEL) &
                    (weights_df[PathSet.WEIGHTS_GROWTH_LOGISTIC_MAX] <= 0)
                ]) == 0)
            assert (
                len(weights_df[
                    (weights_df[PathSet.WEIGHTS_GROWTH_TYPE] == PathSet.LOGISTIC_GROWTH_MODEL) &
                    (weights_df[PathSet.WEIGHTS_GROWTH_LOGISTIC_MID] <= 0)
                ]) == 0)

        return weights_df


    @staticmethod
    def write_configuration(output_dir):
        """
        Write the configuration parameters to function as a record with the output.
        """
        parser = configparser.SafeConfigParser()
        parser.add_section('fasttrips')
        parser.set('fasttrips','input_demand_dir',              Assignment.INPUT_DEMAND_DIR)
        parser.set('fasttrips','input_network_dir',             Assignment.INPUT_NETWORK_ARCHIVE)
        parser.set('fasttrips','input_weights',                 Assignment.INPUT_WEIGHTS)
        if Assignment.CONFIGURATION_FUNCTIONS_FILE:
            parser.set('fasttrips','input_functions',           Assignment.CONFIGURATION_FUNCTIONS_FILE)
        parser.set('fasttrips','run_config',                    Assignment.CONFIGURATION_FILE)


        parser.set('fasttrips','max_iterations',                '%d' % Assignment.MAX_ITERATIONS)
        parser.set('fasttrips','max_pf_iterations',                '%d' % Assignment.MAX_PF_ITERATIONS)
        parser.set('fasttrips','simulation',                    'True' if Assignment.SIMULATION else 'False')
        parser.set('fasttrips','output_dir',                    Assignment.OUTPUT_DIR)
        parser.set('fasttrips','output_passenger_trajectories', 'True' if Assignment.OUTPUT_PASSENGER_TRAJECTORIES else 'False')
        parser.set('fasttrips','output_pathset_per_sim_iter',   'True' if Assignment.OUTPUT_PATHSET_PER_SIM_ITER else 'False')
        parser.set('fasttrips','create_skims',                  'True' if Assignment.CREATE_SKIMS else 'False')
        parser.set('fasttrips','skim_start_time',               Assignment.SKIM_START_TIME.strftime('%H:%M'))
        parser.set('fasttrips','skim_end_time',                 Assignment.SKIM_END_TIME.strftime('%H:%M'))
        parser.set('fasttrips','capacity_constraint',           'True' if Assignment.CAPACITY_CONSTRAINT else 'False')
        parser.set('fasttrips','skip_person_ids',               '%s' % str(Assignment.SKIP_PERSON_IDS))
        parser.set('fasttrips','trace_ids',                     '%s' % str(Assignment.TRACE_IDS))
        parser.set('fasttrips','debug_trace_only',              'True' if Assignment.DEBUG_TRACE_ONLY else 'False')
        parser.set('fasttrips','debug_num_trips',               '%d' % Assignment.DEBUG_NUM_TRIPS)
        parser.set('fasttrips','debug_output_columns',          'True' if Assignment.DEBUG_OUTPUT_COLUMNS else 'False')
        parser.set('fasttrips','fare_zone_symmetry',            'True' if Assignment.FARE_ZONE_SYMMETRY else 'False')
        parser.set('fasttrips','prepend_route_id_to_trip_id',   'True' if Assignment.PREPEND_ROUTE_ID_TO_TRIP_ID else 'False')
        parser.set('fasttrips','number_of_processes',           '%d' % Assignment.NUMBER_OF_PROCESSES)
        parser.set('fasttrips','bump_buffer',                   '%f' % (Assignment.BUMP_BUFFER.total_seconds()/60.0))
        parser.set('fasttrips','bump_one_at_a_time',            'True' if Assignment.BUMP_ONE_AT_A_TIME else 'False')

        #pathfinding
        parser.add_section('pathfinding')
        parser.set('pathfinding','max_num_paths',               '%d' % Assignment.MAX_NUM_PATHS)
        parser.set('pathfinding','min_path_probability',        '%f' % Assignment.MIN_PATH_PROBABILITY)
        parser.set('pathfinding','min_transfer_penalty',        '%f' % PathSet.MIN_TRANSFER_PENALTY)
        parser.set('pathfinding','overlap_chunk_size',          '%d' % PathSet.OVERLAP_CHUNK_SIZE)
        parser.set('pathfinding','overlap_scale_parameter',     '%f' % PathSet.OVERLAP_SCALE_PARAMETER)
        parser.set('pathfinding','overlap_split_transit',       'True' if PathSet.OVERLAP_SPLIT_TRANSIT else 'False')
        parser.set('pathfinding','overlap_variable',            '%s' % PathSet.OVERLAP_VARIABLE)
        parser.set('pathfinding','pathfinding_type',            Assignment.PATHFINDING_TYPE)
        parser.set('pathfinding','pathweights_fixed_width',     'True' if PathSet.WEIGHTS_FIXED_WIDTH else 'False')
        parser.set('pathfinding','stochastic_dispersion',       '%f' % Assignment.STOCH_DISPERSION)
        parser.set('pathfinding','utils_conversion_factor',     '%f' % Assignment.UTILS_CONVERSION)
        parser.set('pathfinding','stochastic_max_stop_process_count', '%d' % Assignment.STOCH_MAX_STOP_PROCESS_COUNT)
        parser.set('pathfinding','stochastic_pathset_size',     '%d' % Assignment.STOCH_PATHSET_SIZE)
        parser.set('pathfinding','time_window',                 '%f' % (Assignment.TIME_WINDOW.total_seconds()/60.0))

        parser.set('pathfinding','transfer_fare_ignore_pathfinding', 'True' if Assignment.TRANSFER_FARE_IGNORE_PATHFINDING else 'False')
        parser.set('pathfinding','transfer_fare_ignore_pathenum',    'True' if Assignment.TRANSFER_FARE_IGNORE_PATHENUM else 'False')

        parser.set('pathfinding','user_class_function',         '%s' % PathSet.USER_CLASS_FUNCTION)

        parser.set('pathfinding','arrive_late_allowed_min',     '%f' % (PathSet.ARRIVE_LATE_ALLOWED_MIN.total_seconds()/60.0))
        parser.set('pathfinding','depart_early_allowed_min',    '%f' % (PathSet.DEPART_EARLY_ALLOWED_MIN.total_seconds()/60.0))

        output_file = open(os.path.join(output_dir, Assignment.CONFIGURATION_OUTPUT_FILE), 'w')
        parser.write(output_file)
        output_file.close()

    @staticmethod
    def initialize_fasttrips_extension(process_number, output_dir, stop_times_df):
        """
        Initialize the C++ fasttrips extension by passing it the network supply.
        """
        FastTripsLogger.debug("Initializing fasttrips extension for process number %d" % process_number)

        # this may not be set yet if it is iter1
        overcap_col = Trip.SIM_COL_VEH_OVERCAP
        if Assignment.MSA_RESULTS:
            overcap_col = Trip.SIM_COL_VEH_MSA_OVERCAP

        if overcap_col not in list(stop_times_df.columns.values):
            stop_times_df[overcap_col] = 0

        FastTripsLogger.debug("initialize_fasttrips_extension() overcap sum: %d" % stop_times_df[overcap_col].sum())
        FastTripsLogger.debug("initialize_fasttrips_extension() STOPTIMES_COLUMN_DEPARTURE_TIME_MIN len: %d mean: %f" % \
                              (len(stop_times_df), stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN].mean()))

        _fasttrips.initialize_supply(output_dir, process_number,
                                     stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].to_numpy().astype('int32'),
                                     stop_times_df[[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                                                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
                                                    Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED,
                                                    overcap_col]].to_numpy().astype('float64'))

        _fasttrips.initialize_parameters(Assignment.TIME_WINDOW.total_seconds()/ 60.0,
                                         Assignment.BUMP_BUFFER.total_seconds()/ 60.0,
                                         Assignment.UTILS_CONVERSION,
                                         PathSet.DEPART_EARLY_ALLOWED_MIN.total_seconds()/ 60.0,
                                         PathSet.ARRIVE_LATE_ALLOWED_MIN.total_seconds()/ 60.0,
                                         Assignment.STOCH_PATHSET_SIZE,
                                         Assignment.STOCH_DISPERSION,
                                         Assignment.STOCH_MAX_STOP_PROCESS_COUNT,
                                         1 if Assignment.TRANSFER_FARE_IGNORE_PATHFINDING else 0,
                                         1 if Assignment.TRANSFER_FARE_IGNORE_PATHENUM else 0,
                                         Assignment.MAX_NUM_PATHS,
                                         Assignment.MIN_PATH_PROBABILITY)

    @staticmethod
    def set_fasttrips_bump_wait(bump_wait_df):
        """
        Sends the bump wait information to the fasttrips extension
        """
        # send a clear message?
        if type(bump_wait_df)==type(None): return

        if len(bump_wait_df) == 0: return

        _fasttrips.set_bump_wait(bump_wait_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                               Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                               Trip.STOPTIMES_COLUMN_STOP_ID_NUM]].astype(np.int32).to_numpy(),
                                 bump_wait_df[Passenger.PF_COL_PAX_A_TIME_MIN].values.astype(np.float64))
    @staticmethod
    def write_vehicle_trips(output_dir, iteration, pathfinding_iteration, simulation_iteration, veh_trips_df):
        """
        """
        columns = ["iteration",                             # we'll add
                   "pathfinding_iteration",
                   "simulation_iteration",
                   Trip.TRIPS_COLUMN_DIRECTION_ID,
                   Trip.TRIPS_COLUMN_SERVICE_ID,
                   Trip.TRIPS_COLUMN_ROUTE_ID,
                   Trip.STOPTIMES_COLUMN_TRIP_ID,
                   Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                   Trip.STOPTIMES_COLUMN_STOP_ID,
                   Trip.STOPTIMES_COLUMN_ARRIVAL_TIME,
                   Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
                   Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,
                   Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
                   Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC,
                   Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC,
                   Trip.VEHICLES_COLUMN_TOTAL_CAPACITY,
                   Trip.SIM_COL_VEH_BOARDS,
                   Trip.SIM_COL_VEH_ALIGHTS,
                   Trip.SIM_COL_VEH_ONBOARD,
                   Trip.SIM_COL_VEH_STANDEES,
                   Trip.SIM_COL_VEH_FRICTION,
                   Trip.SIM_COL_VEH_OVERCAP,
                   # Trip.SIM_COL_VEH_MSA_BOARDS,
                   # Trip.SIM_COL_VEH_MSA_ALIGHTS,
                   # Trip.SIM_COL_VEH_MSA_ONBOARD,
                   # Trip.SIM_COL_VEH_MSA_STANDEES,
                   # Trip.SIM_COL_VEH_MSA_FRICTION,
                   # Trip.SIM_COL_VEH_MSA_OVERCAP
                   ]

        # these may not be in there since they're optional
        for optional_col in [Trip.TRIPS_COLUMN_DIRECTION_ID,
                             Trip.VEHICLES_COLUMN_TOTAL_CAPACITY]:
            if optional_col not in veh_trips_df.columns.values:
                columns.remove(optional_col)

        veh_trips_df[            "iteration"] = iteration
        veh_trips_df["pathfinding_iteration"] = pathfinding_iteration
        veh_trips_df[ "simulation_iteration"] =  simulation_iteration
        Util.write_dataframe(veh_trips_df[columns], "veh_trips_df", os.path.join(output_dir, "veh_trips.csv"),
                             append=(iteration>0 or pathfinding_iteration>0))
        veh_trips_df.drop(["iteration","pathfinding_iteration","simulation_iteration"], axis=1, inplace=True)

    @staticmethod
    def merge_pathsets(pathfind_trip_list_df, pathset_paths_df, pathset_links_df, new_pathset_paths_df, new_pathset_links_df):
        """
        Merge the given new pathset paths and links into the existing
        """
        FastTripsLogger.debug("merge_pathsets():     pathset_paths_df len=%d head=\n%s" % (len(    pathset_paths_df),     pathset_paths_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets(): new_pathset_paths_df len=%d head=\n%s" % (len(new_pathset_paths_df), new_pathset_paths_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets() dtypes=\n%s" % str(pathset_paths_df.dtypes))
        FastTripsLogger.debug("merge_pathsets():     pathset_links_df len=%d head=\n%s" % (len(    pathset_links_df),     pathset_links_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets(): new_pathset_links_df len=%d head=\n%s" % (len(new_pathset_links_df), new_pathset_links_df.head().to_string()))
        FastTripsLogger.debug("merge_pathsets() dtypes=\n%s" % str(pathset_links_df.dtypes))


        FastTripsLogger.debug("merge_pathsets():     pathfind_trip_list_df len=%d head=\n%s" % (len(    pathfind_trip_list_df),     pathfind_trip_list_df.head().to_string()))
        # TODO: This might be inefficient...

        # filter out the new pathset person trips from pathset_paths_df
        pathset_paths_df = pd.merge(left     =pathset_paths_df,
                                        right    =pathfind_trip_list_df[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                        how      ="left",
                                        indicator=True)
        pathset_paths_df = pathset_paths_df.loc[pathset_paths_df["_merge"]=="left_only"]
        pathset_paths_df.drop(["_merge"], axis=1, inplace=True)
        FastTripsLogger.debug("Filtered to %d pathset_paths_df rows" % len(pathset_paths_df))
        # TODO: error prone, make this cleaner with where it's initialized elsewhere
        new_pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN ] = pd.Categorical([Assignment.CHOSEN_NOT_CHOSEN_YET]*len(new_pathset_paths_df),
                                                                                  categories=Assignment.CHOSEN_CATEGORIES, ordered=True)
        new_pathset_paths_df[Assignment.SIM_COL_MISSED_XFER] = 0
        # append
        pathset_paths_df = pd.concat([pathset_paths_df, new_pathset_paths_df], axis=0)
        FastTripsLogger.debug("Concatenated so pathset_paths_df has %d rows" % len(pathset_paths_df))

        # filter out the new pathset person trips from pathset_links_df
        pathset_links_df = pd.merge(left     =pathset_links_df,
                                        right    =pathfind_trip_list_df[[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]],
                                        how      ="left",
                                        indicator=True)
        pathset_links_df = pathset_links_df.loc[pathset_links_df["_merge"]=="left_only"]
        pathset_links_df.drop(["_merge"], axis=1, inplace=True)
        FastTripsLogger.debug("Filtered to %d pathset_links_df rows" % len(pathset_links_df))
        # append
        pathset_links_df = pd.concat([pathset_links_df, new_pathset_links_df], axis=0)
        FastTripsLogger.debug("Concatenated so pathset_links_df has %d rows" % len(pathset_links_df))

        FastTripsLogger.debug("merge_pathsets():     pathset_paths_df len=%d head=\n%s\ntail=\n%s" % (len(pathset_paths_df), pathset_paths_df.head().to_string(),pathset_paths_df.tail().to_string()))
        FastTripsLogger.debug("merge_pathsets():     pathset_links_df len=%d head=\n%s\ntail=\n%s" % (len(pathset_links_df), pathset_links_df.head().to_string(),pathset_links_df.tail().to_string()))

        # done with this
        return (pathset_paths_df, pathset_links_df)

    @staticmethod
    def number_of_pathsets(pathset_paths_df):
        """
        Counts the number of passenger trips with pathsets and returns it.
        """
        return len(pathset_paths_df.groupby([Passenger.PERSONS_COLUMN_PERSON_ID,Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID]))

    @staticmethod
    def assign_paths(output_dir, FT):
        """
        Finds the paths for the passengers.
        """
        # clear any state
        _fasttrips.reset()

        # write the initial load profile, iteration 0
        veh_trips_df     = FT.trips.get_full_trips()
        pathset_paths_df = None
        pathset_links_df = None

        last_chosen_df = pd.DataFrame(columns=[
            Passenger.PERSONS_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.PF_COL_DESCRIPTION
        ])

        success_df = pd.DataFrame(columns=[
            Passenger.PERSONS_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.PF_COL_DESCRIPTION,
            PathSet.SUCCESS_FLAG_COLUMN
        ])

        bump_df = pd.DataFrame(columns=[
            Passenger.PERSONS_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.PF_COL_DESCRIPTION,
            PathSet.BUMP_FLAG_COLUMN
        ])

        success_df[PathSet.SUCCESS_FLAG_COLUMN] = success_df[PathSet.SUCCESS_FLAG_COLUMN].astype(np.float64)
        bump_df[PathSet.BUMP_FLAG_COLUMN] = bump_df[PathSet.BUMP_FLAG_COLUMN].astype(np.float64)

        # write 0-iter vehicle trips
        Assignment.write_vehicle_trips(output_dir, 0, 0, 0, veh_trips_df)

        for iteration in range(1, Assignment.MAX_ITERATIONS+1):

            for pathfinding_iteration in range(1, Assignment.MAX_PF_ITERATIONS + 1):

                # First pathfinding_iteration, find paths for everyone
                if pathfinding_iteration == 1:
                    Assignment.PATHFINDING_EVERYONE = True
                # Subsequent: just find paths for those without paths
                else:
                    Assignment.PATHFINDING_EVERYONE = False

                FastTripsLogger.info("***************************** ITERATION %d PATHFINDING ITERATION %d **************************************" % (iteration, pathfinding_iteration))

                if (Assignment.PATHFINDING_TYPE == Assignment.PATHFINDING_TYPE_READ_FILE) and (iteration == 1) and (pathfinding_iteration == 1):
                    FastTripsLogger.info("Reading paths from file")
                    FT.performance.record_step_start(iteration, pathfinding_iteration, -1, "pathreading")
                    (new_pathset_paths_df, new_pathset_links_df) = FT.passengers.read_passenger_pathsets(output_dir, FT.stops, FT.routes.modes_df, include_asgn=False)
                    num_new_paths_found = Assignment.number_of_pathsets(new_pathset_paths_df)

                    # todo: what about subsequent iterations
                    Assignment.PATHFINDING_TYPE = Assignment.PATHFINDING_TYPE_STOCHASTIC

                else:
                    FT.performance.record_step_start(iteration, pathfinding_iteration, -1, "pathfinding")
                    num_new_paths_found = Assignment.generate_pathsets(FT, pathset_paths_df, veh_trips_df, output_dir, iteration, pathfinding_iteration)
                    (new_pathset_paths_df, new_pathset_links_df) = FT.passengers.setup_passenger_pathsets(iteration, pathfinding_iteration, FT.stops,
                                                                                                          FT.trips.trip_id_df, FT.trips.trips_df, FT.routes.modes_df,
                                                                                                          FT.transfers, FT.tazs, Assignment.PREPEND_ROUTE_ID_TO_TRIP_ID)
                    # write pathfinding results to special PF results file
                    Passenger.write_paths(output_dir, iteration, pathfinding_iteration, -1, new_pathset_paths_df, False,
                                          Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, False)
                    Passenger.write_paths(output_dir, iteration, pathfinding_iteration, -1, new_pathset_links_df, True,
                                          Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, False)

                    # write performance info right away in case we crash, quit, etc
                    FT.performance.write_pathfinding(output_dir, append=((iteration>1) or (pathfinding_iteration>1)))

                # If we found paths for everyone, excellent
                if Assignment.PATHFINDING_EVERYONE:
                    pathset_paths_df = new_pathset_paths_df
                    pathset_links_df = new_pathset_links_df
                # Otherwise, merge with those for whom we already have
                else:
                    (pathset_paths_df, pathset_links_df) = Assignment.merge_pathsets(FT.passengers.pathfind_trip_list_df, pathset_paths_df, pathset_links_df, new_pathset_paths_df, new_pathset_links_df)

                # if we have new paths, simulate them
                if num_new_paths_found > 0:
                    pathset_paths_df, pathset_links_df = Assignment.merge_prior_choices(pathset_paths_df, pathset_links_df, success_df)

                    if Assignment.SIMULATION:
                        FastTripsLogger.info("***************************** ITERATION %d PATHFINDING ITERATION %d *** SIMULATING ***********************" % (iteration, pathfinding_iteration))
                        FT.performance.record_step_start(iteration, pathfinding_iteration, -1, "simulating")
                        (num_passengers_arrived, pathset_paths_df, pathset_links_df, veh_trips_df) = \
                            Assignment.simulate(FT, output_dir, iteration, pathfinding_iteration, pathset_paths_df, pathset_links_df, veh_trips_df)
                    else:
                        # if we're not simulating, we can still calculate costs and choose paths
                        FastTripsLogger.info("***************************** ITERATION %d PATHFINDING ITERATION %d *****CHOOSING PATHS WITHOUT SIMULATING" % (iteration, pathfinding_iteration))
                        FT.performance.record_step_start(iteration, pathfinding_iteration, -1, "choosing_without_simulating")
                        (num_passengers_arrived, pathset_paths_df, pathset_links_df) = \
                            Assignment.choose_paths_without_simulation(FT, output_dir, iteration, pathfinding_iteration, pathset_paths_df, pathset_links_df, veh_trips_df)

                FT.performance.record_step_start(iteration, pathfinding_iteration, -1, "output_per_pathfinding_iteration")

                # Set new schedule
                FT.trips.stop_times_df = veh_trips_df

                # todo: pass back correct simulation iteration?
                Assignment.write_vehicle_trips(output_dir, iteration, pathfinding_iteration, "final", veh_trips_df)

                if Assignment.OUTPUT_PASSENGER_TRAJECTORIES:
                    PathSet.write_path_times(Passenger.get_chosen_links(pathset_links_df), output_dir)

                # capacity gap stuff
                num_paths_found = Assignment.number_of_pathsets(pathset_paths_df)
                num_bumped_passengers = num_paths_found - num_passengers_arrived


                FastTripsLogger.info("")
                FastTripsLogger.info("  Length of trip list:       %10d" % len(FT.passengers.trip_list_df))
                FastTripsLogger.info("  Number of pathsets found:  %10d" % num_paths_found)
                FastTripsLogger.info("  ARRIVED PASSENGERS:        %10d" % num_passengers_arrived)
                FastTripsLogger.info("  MISSED PASSENGERS:         %10d" % num_bumped_passengers)


                FT.performance.record_step_end(iteration, pathfinding_iteration, -1)

                # if no new paths found, pathfinding_iteration loop is done
                if num_new_paths_found == 0:
                    break

            success_df, bump_df = Assignment.save_choices(pathset_paths_df, success_df, bump_df)
            new_choices, last_chosen_df = Assignment.compare_choices(pathset_paths_df, last_chosen_df)
            capacity_gap = (1.0 * new_choices + num_bumped_passengers) / len(FT.passengers.trip_list_df)

            FastTripsLogger.info("###OUTER LOOP: Iteration {}###".format(iteration))
            FastTripsLogger.info("  CAPACITY GAP:              %10.5f" % capacity_gap)
            FastTripsLogger.info("  NEW CHOICE FROM PRIOR:     %10d" % new_choices)


            success_df, bump_df = Assignment.save_choices(pathset_paths_df, success_df, bump_df)
            new_choices, last_chosen_df = Assignment.compare_choices(pathset_paths_df, last_chosen_df)
            capacity_gap = (1.0 * new_choices + num_bumped_passengers) / len(FT.passengers.trip_list_df)

            FastTripsLogger.info("###OUTER LOOP: Iteration {}###".format(iteration))
            FastTripsLogger.info("  CAPACITY GAP:              %10.5f" % capacity_gap)
            FastTripsLogger.info("  NEW CHOICE FROM PRIOR:     %10d" % new_choices)


            success_df, bump_df = Assignment.save_choices(pathset_paths_df, success_df, bump_df)
            new_choices, last_chosen_df = Assignment.compare_choices(pathset_paths_df, last_chosen_df)
            capacity_gap = (1.0 * new_choices + num_bumped_passengers) / len(FT.passengers.trip_list_df)

            FastTripsLogger.info("###OUTER LOOP: Iteration {}###".format(iteration))
            FastTripsLogger.info("  CAPACITY GAP:              %10.5f" % capacity_gap)
            FastTripsLogger.info("  NEW CHOICE FROM PRIOR:     %10d" % new_choices)


            # end condition for iterations loop
            if capacity_gap < Assignment.CONVERGENCE_GAP:
                break

            # end for loop

        return {"capacity_gap": capacity_gap,
                "paths_found": num_paths_found,
                "passengers_arrived": num_passengers_arrived,
                "passengers_missed": num_bumped_passengers,
                "passengers_demand": len(FT.passengers.trip_list_df) }


    @staticmethod
    def compare_choices(pathset_paths_df, prior_choice):
        """
        Reads in a pathset_path_df and the pathset_path_df of a prior iteration
        and returns the number of new choices made in the current iteration
        along with a simplified chosen df to use in the next iteration.

        :param pathset_paths_df: Current iteration of Pathset_Paths_DF
        :param prior_choice: Pathset_Paths_DF of a prior iteration.
        :return: new_choices_count: Total number of new choices in this iteration
                                    compared to the input prior choice df.
                 chosen: Simplified DF of 'person_id', 'person_trip_id_num', and 'description'
                         that can be used in subsequent invocations of this method.
        """

        chosen = Passenger.get_chosen_links(pathset_paths_df)[
            [Passenger.PERSONS_COLUMN_PERSON_ID,
             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
             Passenger.PF_COL_DESCRIPTION]
        ]
        match_choices = pd.merge(chosen, prior_choice, on=[
            Passenger.PERSONS_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.PF_COL_DESCRIPTION], how='inner')

        new_choices_count = chosen.shape[0] - match_choices.shape[0]

        return new_choices_count, chosen


    @staticmethod
    def save_choices(pathset_paths_df, success_df, bump_df):
        """
        Returns an updated count of successfully chosen paths and
        paths that were bumped for each user.
        :param pathset_paths_df: Current iteration of pathset_paths_df after simulation.
        :param success_df: Count of chosen paths for each user across prior iterations
        :param bump_df: Count of bumped paths for each user across prior iterations
        :return:
            success_df: Updated count of chosen paths for each user across each iteration
            bump_df: Updated count of bumped paths for each user across each iteration
        """

        chosen = Passenger.get_chosen_links(pathset_paths_df)
        iter_bump_df = chosen[chosen[Assignment.SIM_COL_PAX_BUMP_ITER] >= 0][
            [Passenger.PERSONS_COLUMN_PERSON_ID,
             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
             Passenger.PF_COL_DESCRIPTION]
        ]
        iter_bump_df[PathSet.BUMP_FLAG_COLUMN] = 1
        iter_bump_df = pd.concat([bump_df, iter_bump_df])
        bump_df = iter_bump_df.groupby([Passenger.PERSONS_COLUMN_PERSON_ID,
                                        Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                        Passenger.PF_COL_DESCRIPTION])[PathSet.BUMP_FLAG_COLUMN].sum().reset_index()

        iter_success_df = chosen[chosen[Assignment.SIM_COL_PAX_BUMP_ITER].isnull()][
            [Passenger.PERSONS_COLUMN_PERSON_ID,
             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
             Passenger.PF_COL_DESCRIPTION]
        ]
        iter_success_df[PathSet.SUCCESS_FLAG_COLUMN] = 1
        iter_success_df = pd.concat([success_df, iter_success_df])

        success_df = iter_success_df.groupby([Passenger.PERSONS_COLUMN_PERSON_ID,
                                              Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                              Passenger.PF_COL_DESCRIPTION]
                                             )[PathSet.SUCCESS_FLAG_COLUMN].sum().reset_index()

        return success_df, bump_df


    @staticmethod
    def merge_prior_choices(pathset_paths_df, pathset_links_df, flag_df):
        """
        Join success or bump flag count information onto a pathset_paths_df and pathset_links_df
        :param pathset_paths_df: Current iteration of pathset_paths_df.
        :param pathset_links_df: Current iteration of pathset_links_df
        :param flag_df: success or bump df
        :return: pathset_paths_df and pathset_links_df with additional flag column.
        """

        assert((PathSet.SUCCESS_FLAG_COLUMN in flag_df) or (PathSet.BUMP_FLAG_COLUMN in flag_df))

        flag_col = PathSet.SUCCESS_FLAG_COLUMN if PathSet.SUCCESS_FLAG_COLUMN in flag_df else PathSet.BUMP_FLAG_COLUMN

        if flag_col in pathset_paths_df:
            pathset_paths_df.drop(labels=[flag_col], axis=1, inplace=True)

        if flag_col in pathset_links_df:
            pathset_links_df.drop(labels=[flag_col], axis=1, inplace=True)

        pathset_paths_df = pd.merge(pathset_paths_df, flag_df,
                                    on = [
                                        Passenger.PERSONS_COLUMN_PERSON_ID,
                                        Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                        Passenger.PF_COL_DESCRIPTION], how = 'left')
        pathset_paths_df.loc[pathset_paths_df[flag_col].isnull(), flag_col] = 0

        pathset_links_df = pd.merge(pathset_links_df,pathset_paths_df[
            ['trip_list_id_num', Passenger.PF_COL_PATH_NUM, flag_col, ]],
                                    on = ['trip_list_id_num', Passenger.PF_COL_PATH_NUM], how = 'left')

        return pathset_paths_df, pathset_links_df

    @staticmethod
    def filter_trip_list_to_not_arrived(trip_list_df, pathset_paths_df):
        """
        Filter the given trip list to only those that have not arrived according to *pathset_paths_df*.
        """
        FastTripsLogger.debug("filter_trip_list_to_not_arrived(): trip_list_df len=%d head()=\n%s"  % (len(trip_list_df), trip_list_df.head().to_string()))
        FastTripsLogger.debug("filter_trip_list_to_not_arrived(): pathset_paths_df len=%d head()=\n%s"  % (len(pathset_paths_df), pathset_paths_df.head().to_string()))
        FastTripsLogger.debug("pathset_paths_df.dtypes")

        # filter to only the chosen paths
        chosen_paths_df = pathset_paths_df.loc[pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN] > Assignment.CHOSEN_NOT_CHOSEN_YET,
                                                [Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Assignment.SIM_COL_PAX_CHOSEN]]

        # add chosen index
        trip_list_df_to_return = pd.merge(left  =trip_list_df,
                                              right =chosen_paths_df,
                                              how   ="left")
        # use it to filter to null chosen
        trip_list_df_to_return = trip_list_df_to_return.loc[pd.isnull(trip_list_df_to_return[Assignment.SIM_COL_PAX_CHOSEN])]
        # remove chosen column
        trip_list_df_to_return.drop([Assignment.SIM_COL_PAX_CHOSEN], axis=1, inplace=True)

        FastTripsLogger.debug("filter_trip_list_to_not_arrived(): trip_list_df_to_return len=%d head()=\n%s"  % (len(trip_list_df_to_return), trip_list_df_to_return.head().to_string()))
        return trip_list_df_to_return

    @staticmethod
    def generate_pathsets(FT, pathset_paths_df, veh_trips_df, output_dir, iteration, pathfinding_iteration):
        """
        Figures out which person trips for whom to generate_pathsets, stored in :py:attr:`Passenger.pathfind_trip_list_df`

        Generates paths sets for those person trips using deterministic trip-based shortest path (TBSP) or
        stochastic trip-based hyperpath (TBHP).

        Returns the number of pathsets found.
        """
        FastTripsLogger.info("**************************** GENERATING PATHS **********************************************************")
        start_time          = datetime.datetime.now()
        process_dict        = {}  # workernum -> {"process":process, "alive":alive bool, "done":done bool, "working_on":(person_id, trip_list_num)}
        todo_queue          = None
        done_queue          = None

        # We only need to do this once
        if iteration == 1 and pathfinding_iteration == 1:
            if Assignment.DEBUG_TRACE_ONLY:
                FT.passengers.trip_list_df = FT.passengers.trip_list_df.loc[FT.passengers.trip_list_df[Passenger.TRIP_LIST_COLUMN_TRACE]==True]
            else:
                if Assignment.DEBUG_NUM_TRIPS > 0 and len(FT.passengers.trip_list_df) > Assignment.DEBUG_NUM_TRIPS:
                    FastTripsLogger.info("Truncating trip list to %d trips" % Assignment.DEBUG_NUM_TRIPS)
                    FT.passengers.trip_list_df = FT.passengers.trip_list_df.iloc[:Assignment.DEBUG_NUM_TRIPS]

            # Skip someone?
            if Assignment.SKIP_PERSON_IDS and len(Assignment.SKIP_PERSON_IDS) > 0:
                FT.passengers.trip_list_df = FT.passengers.trip_list_df.loc[~FT.passengers.trip_list_df[Passenger.TRIP_LIST_COLUMN_PERSON_ID].isin(Assignment.SKIP_PERSON_IDS)]

        # these are the trips for which we'll find paths
        FT.passengers.pathfind_trip_list_df = FT.passengers.trip_list_df

        if Assignment.PATHFINDING_EVERYONE:
            # we're starting over with empty vehicles
            Trip.reset_onboard(veh_trips_df)
        else:
            FastTripsLogger.info("Finding paths for trips for those that haven't arrived yet")
            FT.passengers.pathfind_trip_list_df = Assignment.filter_trip_list_to_not_arrived(FT.passengers.trip_list_df, pathset_paths_df)

        est_paths_to_find   = len(FT.passengers.pathfind_trip_list_df)
        FastTripsLogger.info("Finding pathsets for %d trips" % est_paths_to_find)
        if est_paths_to_find == 0:
            return 0

        info_freq           = pow(10, int(math.log(est_paths_to_find+1,10)-1))
        if info_freq < 1: info_freq = 1
        # info_freq = 1 # DEBUG CRASH

        num_processes       = Assignment.NUMBER_OF_PROCESSES
        if  Assignment.NUMBER_OF_PROCESSES < 1:
            num_processes   = multiprocessing.cpu_count()
        # it's not worth it unless each process does 3
        if num_processes > est_paths_to_find*3:
            num_processes = int(est_paths_to_find//3)

        # this is probalby time consuming... put in a try block
        try:
            # Setup multiprocessing processes
            if num_processes > 1:
                todo_queue      = multiprocessing.Queue()
                done_queue      = multiprocessing.Queue()
                for process_idx in range(1, 1+num_processes):
                    FastTripsLogger.info("Starting worker process %2d" % process_idx)
                    process_dict[process_idx] = {
                        "process":multiprocessing.Process(target=find_trip_based_paths_process_worker,
                            args=(iteration, pathfinding_iteration, process_idx, Assignment.INPUT_NETWORK_ARCHIVE, Assignment.INPUT_DEMAND_DIR,
                                  Assignment.CONFIGURATION_FILE, Assignment.CONFIGURATION_FUNCTIONS_FILE,
                                  Assignment.OUTPUT_DIR, todo_queue, done_queue,
                                  Assignment.PATHFINDING_TYPE==Assignment.PATHFINDING_TYPE_STOCHASTIC,
                                  Assignment.bump_wait_df, veh_trips_df)),
                        "alive":True,
                        "done":False
                    }
                    process_dict[process_idx]["process"].start()
            else:
                Assignment.initialize_fasttrips_extension(0, output_dir, veh_trips_df)

            # process tasks or send tasks to workers for processing
            num_paths_found_prev  = 0
            num_paths_found_now   = 0
            num_paths_sought      = 0
            path_cols             = list(FT.passengers.pathfind_trip_list_df.columns.values)
            for path_tuple in FT.passengers.pathfind_trip_list_df.itertuples(index=False):
                path_dict         = dict(list(zip(path_cols, path_tuple)))
                trip_list_id      = path_dict[Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM]
                person_id         = path_dict[Passenger.TRIP_LIST_COLUMN_PERSON_ID]
                person_trip_id    = path_dict[Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID]
                do_trace          = path_dict[Passenger.TRIP_LIST_COLUMN_TRACE]

                if Assignment.DEBUG_TRACE_ONLY and not do_trace: continue

                # first iteration -- create path objects
                if iteration==1:
                    trip_pathset = PathSet(path_dict)
                    FT.passengers.add_pathset(trip_list_id, trip_pathset)
                else:
                    trip_pathset = FT.passengers.get_pathset(trip_list_id)

                if not trip_pathset.goes_somewhere(): continue

                # find pathsets for everyone -- dwell times have changed
                # if iteration > 1 and trip_list_id not in Assignment.bumped_trip_list_nums:
                #    num_paths_found_prev += 1
                #    continue

                if num_processes > 1:
                    todo_queue.put( trip_pathset )
                else:
                    if do_trace:
                        FastTripsLogger.debug("Tracing assignment of person_id %s and trip %s" % (person_id, person_trip_id))

                    # do the work
                    (pathdict, perf_dict) = \
                        Assignment.find_trip_based_pathset(iteration, pathfinding_iteration, trip_pathset,
                                                           Assignment.PATHFINDING_TYPE==Assignment.PATHFINDING_TYPE_STOCHASTIC,
                                                           trace=do_trace)
                    num_paths_sought += 1

                    trip_pathset.pathdict = pathdict
                    FT.performance.add_info(iteration, pathfinding_iteration, person_id, person_trip_id, perf_dict)

                    if trip_pathset.path_found():
                        num_paths_found_now += 1

                    if num_paths_sought % info_freq == 0:
                        time_elapsed = datetime.datetime.now() - start_time
                        FastTripsLogger.info(" %6d paths sought, %6d paths found of %d paths total.  Time elapsed: %2dh:%2dm:%2ds" % (
                                             num_paths_sought, num_paths_found_now, est_paths_to_find,
                                             int( time_elapsed.total_seconds()/ 3600),
                                             int( (time_elapsed.total_seconds() % 3600)/ 60),
                                             time_elapsed.total_seconds() % 60))

            # multiprocessing follow-up
            if num_processes > 1:
                # we're done, let each process know
                for process_idx in list(process_dict.keys()):
                    todo_queue.put('DONE')

                # get results
                done_procs = 0  # where done means not alive
                while done_procs < len(process_dict):

                    try:
                        result     = done_queue.get(True, 30)
                        worker_num = result[0]

                        # FastTripsLogger.debug("Received %s" % str(result))
                        if result[1] == "DONE":
                            FastTripsLogger.debug("Received done from process %d" % worker_num)
                            process_dict[worker_num]["done"] = True
                        elif result[1] == "STARTING":
                            process_dict[worker_num]["working_on"] = (result[2],result[3])
                        elif result[1] == "COMPLETED":
                            trip_list_id    = result[2]
                            pathset         = FT.passengers.get_pathset(trip_list_id)
                            pathset.pathdict= result[3]
                            perf_dict       = result[4]

                            FT.performance.add_info(iteration, pathfinding_iteration, pathset.person_id, pathset.person_trip_id, perf_dict)

                            num_paths_sought += 1
                            if pathset.path_found():
                                num_paths_found_now += 1

                            if num_paths_sought % info_freq == 0:
                                time_elapsed = datetime.datetime.now() - start_time
                                FastTripsLogger.info("  %6d paths sought, %6d paths found of %d paths total.  Time elapsed: %2dh:%2dm:%2ds" % (
                                                     num_paths_sought, num_paths_found_now, est_paths_to_find,
                                                     int( time_elapsed.total_seconds()/ 3600),
                                                     int( (time_elapsed.total_seconds() % 3600)/ 60),
                                                     time_elapsed.total_seconds() % 60))

                            del process_dict[worker_num]["working_on"]
                        else:
                            print("Unexpected done queue contents: " + str(result))

                    except queue.Empty:
                        # This is normal
                        pass
                    except:
                        FastTripsLogger.error("Caught exception: %s" % str(sys.exc_info()))
                        pass

                    # check if any processes are not alive
                    for process_idx in list(process_dict.keys()):
                        if process_dict[process_idx]["alive"] and not process_dict[process_idx]["process"].is_alive():
                            FastTripsLogger.debug("Process %d is not alive" % process_idx)
                            process_dict[process_idx]["alive"] = False
                            done_procs += 1

                # join up my processes
                for process_idx in list(process_dict.keys()):
                    process_dict[process_idx]["process"].join()

                # check if any processes crashed
                for process_idx in list(process_dict.keys()):
                    if not process_dict[process_idx]["done"]:
                        if "working_on" in process_dict[process_idx]:
                            FastTripsLogger.info("Process %d appears to have crashed; it was working on %s" % \
                                                 (process_idx, str(process_dict[process_idx]["working_on"])))
                        else:
                            FastTripsLogger.info("Process %d appears to have crashed; see ft_debug_worker%02d.log" % (process_idx, process_idx))

        except (KeyboardInterrupt, SystemExit):
            exc_type, exc_value, exc_tb = sys.exc_info()
            FastTripsLogger.error("Exception caught: %s" % str(exc_type))
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines: FastTripsLogger.error(e)
            FastTripsLogger.error("Terminating processes")
            # terminating my processes
            for proc in process_dict:
                proc.terminate()
            raise
        except:
            # some other error
            exc_type, exc_value, exc_tb = sys.exc_info()
            error_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
            for e in error_lines: FastTripsLogger.error(e)
            raise

        time_elapsed = datetime.datetime.now() - start_time
        FastTripsLogger.info("Finished finding %6d passenger paths.  Time elapsed: %2dh:%2dm:%2ds" % (
                                 num_paths_found_now,
                                 int( time_elapsed.total_seconds()/ 3600),
                                 int( (time_elapsed.total_seconds() % 3600)/ 60),
                                 time_elapsed.total_seconds() % 60))

        return num_paths_found_now + num_paths_found_prev


    @staticmethod
    def find_trip_based_pathset(iteration, pathfinding_iteration, pathset, hyperpath, trace):
        """
        Perform trip-based path set search.

        Will do so either backwards (destination to origin) if :py:attr:`PathSet.direction` is :py:attr:`PathSet.DIR_OUTBOUND`
        or forwards (origin to destination) if :py:attr:`PathSet.direction` is :py:attr:`PathSet.DIR_INBOUND`.

        Returns (pathdict,
                 performance_dict)

        Where pathdict maps {pathnum:{PATH_KEY_COST:cost, PATH_KEY_PROBABILITY:probability, PATH_KEY_STATES:[state list]}}

        Where performance_dict includes:
                 pathfinding return status,
                 number of label iterations,
                 max number of times a stop was processed,
                 seconds spent in labeling,
                 seconds spend in enumeration

        :param pathset:   the path to fill in
        :type  pathset:   a :py:class:`PathSet` instance
        :param hyperpath: pass True to use a stochastic hyperpath-finding algorithm, otherwise a deterministic shortest path
                          search algorithm will be use.
        :type  hyperpath: bool
        :param trace:     pass True if this path should be traced to the debug log
        :type  trace:     bool

        """
        # FastTripsLogger.debug("C++ extension start")
        # send it to the C++ extension
        (ret_ints, ret_doubles, path_costs, process_num, pf_returnstatus,
         label_iterations, num_labeled_stops, max_label_process_count,
         ms_labeling, ms_enumerating,
         bytes_workingset, bytes_privateusage, mem_timestamp) = \
            _fasttrips.find_pathset(iteration, pathfinding_iteration, hyperpath, pathset.person_id, pathset.person_trip_id,
                                 pathset.user_class, pathset.purpose, pathset.access_mode, pathset.transit_mode, pathset.egress_mode,
                                 pathset.o_taz_num, pathset.d_taz_num,
                                 1 if pathset.outbound else 0, float(pathset.pref_time_min), pathset.vot,
                                 1 if trace else 0)
        # FastTripsLogger.debug("C++ extension complete")
        FastTripsLogger.debug("Finished finding path for person %s trip %s" % (pathset.person_id, pathset.person_trip_id))
        pathdict = {}
        row_num  = 0

        for path_num in range(path_costs.shape[0]):

            pathdict[path_num] = {}
            pathdict[path_num][PathSet.PATH_KEY_COST       ] = path_costs[path_num, 0]
            pathdict[path_num][PathSet.PATH_KEY_FARE       ] = path_costs[path_num, 1]
            pathdict[path_num][PathSet.PATH_KEY_PROBABILITY] = path_costs[path_num, 2]
            pathdict[path_num][PathSet.PATH_KEY_INIT_COST  ] = path_costs[path_num, 3]
            pathdict[path_num][PathSet.PATH_KEY_INIT_FARE  ] = path_costs[path_num, 4]
            # List of (stop_id, stop_state)
            pathdict[path_num][PathSet.PATH_KEY_STATES     ] = []

            # print "path_num %d" % path_num

            # while we have unprocessed rows and the row is still relevant for this path_num
            while (row_num < ret_ints.shape[0]) and (ret_ints[row_num, 0] == path_num):
                # print row_num

                mode = ret_ints[row_num,2]
                # todo
                if mode == -100:
                    mode = PathSet.STATE_MODE_ACCESS
                elif mode == -101:
                    mode = PathSet.STATE_MODE_EGRESS
                elif mode == -102:
                    mode = PathSet.STATE_MODE_TRANSFER
                elif mode == -103:
                    mode = Passenger.MODE_GENERIC_TRANSIT_NUM

                if hyperpath:
                    pathdict[path_num][PathSet.PATH_KEY_STATES].append( (ret_ints[row_num, 1], [
                        ret_doubles[row_num,0],                                                          # label,
                        Assignment.NETWORK_BUILD_DATE_START_TIME + datetime.timedelta(minutes=ret_doubles[row_num,1]),  # departure/arrival time
                        mode,                                                                            # departure/arrival mode
                        ret_ints[row_num,3],                                                             # trip id
                        ret_ints[row_num,4],                                                             # successor/predecessor
                        ret_ints[row_num,5],                                                             # sequence
                        ret_ints[row_num,6],                                                             # sequence succ/pred
                        datetime.timedelta(minutes=ret_doubles[row_num,2]),                              # link time
                        ret_doubles[row_num,3],                                                          # link fare
                        ret_doubles[row_num,4],                                                          # link cost
                        ret_doubles[row_num,5],                                                          # link distance
                        ret_doubles[row_num,6],                                                          # cost
                        Assignment.NETWORK_BUILD_DATE_START_TIME + datetime.timedelta(minutes=ret_doubles[row_num,7])   # arrival/departure time
                    ] ) )
                else:
                    pathdict[path_num][PathSet.PATH_KEY_STATES].append( (ret_ints[row_num, 1], [
                        datetime.timedelta(minutes=ret_doubles[row_num,0]),                              # label,
                        Assignment.NETWORK_BUILD_DATE_START_TIME + datetime.timedelta(minutes=ret_doubles[row_num,1]),  # departure/arrival time
                        mode,                                                                            # departure/arrival mode
                        ret_ints[row_num,3],                                                             # trip id
                        ret_ints[row_num,4],                                                             # successor/predecessor
                        ret_ints[row_num,5],                                                             # sequence
                        ret_ints[row_num,6],                                                             # sequence succ/pred
                        datetime.timedelta(minutes=ret_doubles[row_num,2]),                              # link time
                        ret_doubles[row_num,3],                                                          # link fare
                        datetime.timedelta(minutes=ret_doubles[row_num,4]),                              # link cost
                        ret_doubles[row_num,5],                                                          # link dist
                        datetime.timedelta(minutes=ret_doubles[row_num,6]),                              # cost
                        Assignment.NETWORK_BUILD_DATE_START_TIME + datetime.timedelta(minutes=ret_doubles[row_num,7])   # arrival/departure time
                    ] ) )
                row_num += 1

        perf_dict = { \
            Performance.PERFORMANCE_PF_COL_PROCESS_NUM           : process_num,
            Performance.PERFORMANCE_PF_COL_PATHFINDING_STATUS    : pf_returnstatus,
            Performance.PERFORMANCE_PF_COL_LABEL_ITERATIONS      : label_iterations,
            Performance.PERFORMANCE_PF_COL_NUM_LABELED_STOPS     : num_labeled_stops,
            Performance.PERFORMANCE_PF_COL_MAX_STOP_PROCESS_COUNT: max_label_process_count,
            Performance.PERFORMANCE_PF_COL_TIME_LABELING_MS      : ms_labeling,
            Performance.PERFORMANCE_PF_COL_TIME_ENUMERATING_MS   : ms_enumerating,
            Performance.PERFORMANCE_PF_COL_TRACED                : trace,
            Performance.PERFORMANCE_PF_COL_WORKING_SET_BYTES     : bytes_workingset,
            Performance.PERFORMANCE_PF_COL_PRIVATE_USAGE_BYTES   : bytes_privateusage,
            Performance.PERFORMANCE_PF_COL_MEM_TIMESTAMP         : datetime.datetime.fromtimestamp(mem_timestamp)
        }
        return (pathdict, perf_dict)


    @staticmethod
    def find_passenger_vehicle_times(pathset_links_df, veh_trips_df):
        """
        Given a dataframe of passenger links and a dataframe of vehicle trip links, adds two new columns to the passenger links for board and alight time.

        - Takes the trip links of pathset_links_df (columns: person_id, trip_list_id_num, pathnum, linkmode, trip_id_num, A_id_num, B_id_num, A_seq, B_seq, pf_A_time, pf_B_time, pf_linktime, A_id, B_id, trip_id)
        - Joins with vehicle trips on trip id num, A_id, A_seq to add:
          * board time   (Assignment.SIM_COL_PAX_BOARD_TIME)
          * overcap      (Assignment.SIM_COL_PAX_OVERCAP)
          * overcap_frac (Assignment.SIM_COL_PAX_OVERCAP_FRAC)
        - Joins with vehicle trips on trip id num, B_id, B_seq to add:
          * alight time (Assignment.SIM_COL_PAX_ALIGHT_TIME)

        Returns the same dataframe but with four additional columns (replacing them if they're already there).
        """
        if False and len(Assignment.TRACE_IDS) > 0:
            FastTripsLogger.debug("find_passenger_vehicle_times(): input pathset_links_df len=%d\n%s" % \
                                  (len(pathset_links_df), pathset_links_df.loc[pathset_links_df[Passenger.TRIP_LIST_COLUMN_TRACE]==True].to_string()))

        if Assignment.SIM_COL_PAX_BOARD_TIME in list(pathset_links_df.columns.values):
            pathset_links_df.drop([Assignment.SIM_COL_PAX_BOARD_TIME,
                                   Assignment.SIM_COL_PAX_ALIGHT_TIME,
                                   Assignment.SIM_COL_PAX_OVERCAP], axis=1, inplace=True)
        if Assignment.SIM_COL_PAX_OVERCAP_FRAC in list(pathset_links_df.columns.values):
            pathset_links_df.drop([Assignment.SIM_COL_PAX_OVERCAP_FRAC], axis=1, inplace=True)

        # FastTripsLogger.debug("pathset_links_df:\n%s\n" % pathset_links_df.head().to_string())
        if False: FastTripsLogger.debug("veh_trips_df:\n%s\n" % veh_trips_df.head().to_string())

        veh_trip_cols = [Trip.STOPTIMES_COLUMN_TRIP_ID,
                         Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                         Trip.STOPTIMES_COLUMN_STOP_ID,
                         Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,
                         Trip.SIM_COL_VEH_OVERCAP,                # TODO: what about msa_overcap?
                         Trip.SIM_COL_VEH_OVERCAP_FRAC]

        # this one may not be here -- it's only present during capacity stuff
        if Trip.SIM_COL_VEH_OVERCAP_FRAC not in list(veh_trips_df.columns.values):
            veh_trip_cols.remove(Trip.SIM_COL_VEH_OVERCAP_FRAC)

        #This is a little long winded, but it cuts down on memory dramatically, but only copying
        #what is actually needed during the merges.
        intermediate = pd.merge(left=pathset_links_df[[Passenger.PERSONS_COLUMN_PERSON_ID, Trip.STOPTIMES_COLUMN_TRIP_ID,'A_id','A_seq',
                                        Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                        Passenger.PF_COL_PATH_NUM,
                                        Passenger.PF_COL_LINK_NUM,
                                        'B_id', 'B_seq']],
                 right=veh_trips_df[veh_trip_cols],
                 left_on=[Trip.STOPTIMES_COLUMN_TRIP_ID, 'A_id', 'A_seq'],
                 right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID,
                          Trip.STOPTIMES_COLUMN_STOP_ID,
                          Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                 how     ='inner')

        intermediate = intermediate.drop(columns=[Trip.STOPTIMES_COLUMN_STOP_ID,
                                                  Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                  'A_id', 'A_seq'])

        intermediate = pd.merge(left=intermediate,
                                right=veh_trips_df[[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID,
                                                    Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]],
                                left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID,'B_id','B_seq'],
                                right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                          Trip.STOPTIMES_COLUMN_STOP_ID,
                                          Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                how     ='inner',)

        intermediate = intermediate.drop(columns=[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                  Trip.STOPTIMES_COLUMN_STOP_ID,
                                                  Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                  'B_id','B_seq'])

        intermediate = intermediate.rename(columns={
            Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:Assignment.SIM_COL_PAX_BOARD_TIME,   # transit vehicle depart time (at A) = board time for pax
            Trip.STOPTIMES_COLUMN_ARRIVAL_TIME  :Assignment.SIM_COL_PAX_ALIGHT_TIME,  # transit vehicle arrive time (at B) = alight time for pax
        })

        if False and len(Assignment.TRACE_IDS) > 0:
            FastTripsLogger.debug("find_passenger_vehicle_times(): output pathset_links_df len=%d\n%s" % \
                                  (len(pathset_links_df), pathset_links_df.loc[pathset_links_df[Passenger.TRIP_LIST_COLUMN_TRACE]==True].to_string()))

        return pd.merge(pathset_links_df, intermediate, on=[Passenger.PERSONS_COLUMN_PERSON_ID,
                                                            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                            Passenger.PF_COL_PATH_NUM,
                                                            Passenger.PF_COL_LINK_NUM,], how='left')


    @staticmethod
    def put_passengers_on_vehicles(pathset_links_df, veh_trips_df):
        """
        Puts the chosen passenger trips specified in pathset_links_df onto the transit vehicle trips specified by veh_trip_df.

        Returns veh_trips_df but with updated columns
          - :py:attr:`Trip.SIM_COL_VEH_BOARDS`
          - :py:attr:`Trip.SIM_COL_VEH_ALIGHTS`
          - :py:attr:`Trip.SIM_COL_VEH_ONBOARD`
          - :py:attr:`Trip.SIM_COL_VEH_OVERCAP`
          - :py:attr:`Trip.SIM_COL_VEH_OVERCAP_FRAC`
        """
        # drop these -- we'll set them
        if Trip.SIM_COL_VEH_BOARDS in list(veh_trips_df.columns.values):
            veh_trips_df.drop([Trip.SIM_COL_VEH_BOARDS,
                               Trip.SIM_COL_VEH_ALIGHTS,
                               Trip.SIM_COL_VEH_ONBOARD], axis=1, inplace=True)

        veh_trips_df_len = len(veh_trips_df)

        passengers_df = Passenger.get_chosen_links(pathset_links_df, transit_only=True, copy=False)

        # only care about trips
        #passengers_df = passengers_df.loc[passengers_df[Passenger.PF_COL_ROUTE_ID].notnull()]

        # Group to boards by counting trip_list_id_nums for a (trip_id, A_id as stop_id)
        passenger_trips_boards = passengers_df.loc[passengers_df[Assignment.SIM_COL_PAX_BUMP_ITER].isnull(),  # unbumped passengers
                                                   [Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id_num','A_seq']].groupby([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,'A_id_num','A_seq']).count()
        passenger_trips_boards.index.names = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                              Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                              Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]

        # And alights by counting path_ids for a (trip_id, B_id as stop_id)
        passenger_trips_alights = passengers_df.loc[passengers_df[Assignment.SIM_COL_PAX_BUMP_ITER].isnull(),
                                                    [Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
                                                     Trip.TRIPS_COLUMN_TRIP_ID_NUM,'B_id_num','B_seq']].groupby([Trip.TRIPS_COLUMN_TRIP_ID_NUM,'B_id_num','B_seq']).count()
        passenger_trips_alights.index.names = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                               Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                               Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]

        # Join them to the transit vehicle trips so we can put people on vehicles (boards)
        veh_loaded_df = pd.merge(left        = veh_trips_df,
                                     right       = passenger_trips_boards,
                                     left_on     = [Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                     right_index = True,
                                     how         = 'left')
        veh_loaded_df.rename(columns={Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM:Trip.SIM_COL_VEH_BOARDS}, inplace=True)


        # Join for alights
        veh_loaded_df = pd.merge(left        = veh_loaded_df,
                                     right       = passenger_trips_alights,
                                    left_on      = [Trip.TRIPS_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                    right_index  = True,
                                    how          ='left')
        veh_loaded_df.rename(columns={Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM:Trip.SIM_COL_VEH_ALIGHTS}, inplace=True)

        # replace has weird behaviour as of now, so we need a hack to take care of the new behaviour of fill in
        # TimeDelta columns --> https://github.com/pandas-dev/pandas/issues/29024
        tdelta_cols = list(veh_loaded_df.select_dtypes(include=['timedelta']).columns)
        for col in tdelta_cols:
            veh_loaded_df[col].fillna(pd.Timedelta(seconds=0), inplace=True)
        for col in veh_loaded_df.columns:
            if col in tdelta_cols:
                continue
            veh_loaded_df[col].fillna(0, inplace=True)

        assert(len(veh_loaded_df)==veh_trips_df_len)

        # these are ints, not floats
        for col in [Trip.SIM_COL_VEH_BOARDS, Trip.SIM_COL_VEH_ALIGHTS]:
            veh_loaded_df.loc[:, col] = veh_loaded_df[col].astype(int)

        veh_loaded_df.set_index([Trip.TRIPS_COLUMN_TRIP_ID_NUM,Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],inplace=True)
        veh_loaded_df[Trip.SIM_COL_VEH_ONBOARD    ] = veh_loaded_df[Trip.SIM_COL_VEH_BOARDS    ] - veh_loaded_df[Trip.SIM_COL_VEH_ALIGHTS    ]

        # on board is the cumulative sum of boards - alights
        trips_cumsum = veh_loaded_df[[Trip.SIM_COL_VEH_ONBOARD]].groupby(level=[0]).cumsum()
        veh_loaded_df.drop([Trip.SIM_COL_VEH_ONBOARD], axis=1, inplace=True) # replace with cumsum
        veh_loaded_df = pd.merge(left        = veh_loaded_df,
                                     right       = trips_cumsum,
                                     left_index  = True,
                                     right_index = True,
                                     how         = 'left')

        assert(len(veh_loaded_df)==veh_trips_df_len)
        # print veh_trips_df.loc[5123368]
        veh_loaded_df.reset_index(inplace=True)

        # overcap = how many people are problematic, or onboard-totalcap.  If negative, we have space.
        # overcap_frac = what percentage of boards are problematic
        veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP     ] = veh_loaded_df[Trip.SIM_COL_VEH_ONBOARD] - veh_loaded_df[Trip.VEHICLES_COLUMN_TOTAL_CAPACITY]
        veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP_FRAC] = 0.0
        veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_BOARDS ]>0, Trip.SIM_COL_VEH_OVERCAP_FRAC] = veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP] / veh_loaded_df[Trip.SIM_COL_VEH_BOARDS]

        FastTripsLogger.debug("veh_loaded_df with onboard>0: (showing head)\n" + \
                              veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_ONBOARD]>0].head().to_string())

        return veh_loaded_df

    @staticmethod
    def flag_missed_transfers(pathset_paths_df, pathset_links_df):
        """
        Given passenger pathset links with the vehicle board_time and alight_time attached to trip links,
        this method will add columns to determine if there are missed transfers.

        This works on all paths in the pathset rather than just the chosen paths because then we can choose
        a path without missed transfers.

        The following columns are used in pathset_links_df:
        * Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
        * Passenger.PF_COL_PATH_NUM,
        * Passenger.PF_COL_LINK_NUM,
        * Assignment.SIM_COL_PAX_BOARD_TIME
        * Assignment.SIM_COL_PAX_ALIGHT_TIME
        * Passenger.PF_COL_PAX_B_TIME

        In particular, the following columns are added (or replaced if they're already there) to *pathset_links_df*:

        ======================================================= ==============================================================================
        Column Name                                             Column Description
        ======================================================= ==============================================================================
        :py:attr:`Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN`      Delay in alight_time from original pathfinding understanding of alight time
        :py:attr:`Assignment.SIM_COL_PAX_A_TIME`                New A time given the trip board/alight times for the trip links
        :py:attr:`Assignment.SIM_COL_PAX_B_TIME`                New B time given the trip board/alight times for the trip links
        :py:attr:`Assignment.SIM_COL_PAX_LINK_TIME`             New link time from B time - A time
        :py:attr:`Assignment.SIM_COL_PAX_WAIT_TIME`             New waittime given the trip board/alight times for the trip links
        :py:attr:`Assignment.SIM_COL_PAX_MISSED_XFER`           Is this link a missed xfer
        ======================================================= ==============================================================================

        The column, :py:attr:`AssignmentSIM_COL_PAX_MISSED_XFER`, is also added to *pathset_paths_df*.

        """
        # Drop these, we'll set them again
        if Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN in list(pathset_links_df.columns.values):
            pathset_links_df.drop([Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN,
                                   Assignment.SIM_COL_PAX_A_TIME,
                                   Assignment.SIM_COL_PAX_B_TIME,
                                   Assignment.SIM_COL_PAX_LINK_TIME,
                                   Assignment.SIM_COL_PAX_WAIT_TIME,
                                   Assignment.SIM_COL_MISSED_XFER], axis=1, inplace=True)

        # Set alight delay (min)
        FastTripsLogger.debug("flag_missed_transfers() pathset_links_df (%d):\n%s" % (len(pathset_links_df), pathset_links_df.head().to_string()))
        pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN] = 0.0
        pathset_links_df.loc[pd.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]), Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN] = \
            ((pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME]-pathset_links_df[Passenger.PF_COL_PAX_B_TIME]) / np.timedelta64(1, 'm'))

        #: todo: is there a more elegant way to take care of this?  some trips have times after midnight so they're the next day
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN]>22*60, Assignment.SIM_COL_PAX_BOARD_TIME ] = pathset_links_df[Assignment.SIM_COL_PAX_BOARD_TIME] - np.timedelta64(24, 'h')
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN]>22*60, Assignment.SIM_COL_PAX_ALIGHT_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME] - np.timedelta64(24, 'h')
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN]>22*60, Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN] = \
            ((pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME]-pathset_links_df[Passenger.PF_COL_PAX_B_TIME]) / np.timedelta64(1, 'm'))

        max_alight_delay_min = pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN].max()
        FastTripsLogger.debug("Biggest alight_delay = %f" % max_alight_delay_min)
        if max_alight_delay_min > 0:
            FastTripsLogger.debug("\n%s" % pathset_links_df.sort_values(by=Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN, ascending=False).head().to_string())

        # For trips, alight_time is the new B_time
        # Set A_time for links AFTER trip links by joining to next leg
        next_trips = pathset_links_df[[
            Passenger.TRIP_LIST_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.PF_COL_PATH_NUM,
            Passenger.PF_COL_LINK_NUM,
            Assignment.SIM_COL_PAX_ALIGHT_TIME]].copy()
        next_trips[Passenger.PF_COL_LINK_NUM] = next_trips[Passenger.PF_COL_LINK_NUM] + 1
        next_trips.rename(columns={Assignment.SIM_COL_PAX_ALIGHT_TIME:Assignment.SIM_COL_PAX_A_TIME}, inplace=True)
        # Add it to passenger trips.  Now A time is set for links after trip links (note this will never be a trip link)
        pathset_links_df = pd.merge(left =pathset_links_df,
                                        right=next_trips,
                                        how  ="left",
                                        on   =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                               Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                               Passenger.PF_COL_PATH_NUM,
                                               Passenger.PF_COL_LINK_NUM])

        FastTripsLogger.debug(str(pathset_links_df.dtypes))

        # Set the new B time for those links -- link time for access/egress/xfer is travel time since wait times are in trip links
        pathset_links_df[Assignment.SIM_COL_PAX_B_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_A_TIME] + pathset_links_df[Passenger.PF_COL_LINK_TIME]
        # For trip links, it's alight time
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_TRIP,   Assignment.SIM_COL_PAX_B_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_TIME]
        # For access links, it doesn't change from the original pathfinding result
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_ACCESS, Assignment.SIM_COL_PAX_A_TIME] = pathset_links_df[Passenger.PF_COL_PAX_A_TIME]
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_ACCESS, Assignment.SIM_COL_PAX_B_TIME] = pathset_links_df[Passenger.PF_COL_PAX_B_TIME]

        # Now we only need to set the trip link's A time from the previous link's new_B_time
        next_trips = pathset_links_df[[
            Passenger.TRIP_LIST_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.PF_COL_PATH_NUM,
            Passenger.PF_COL_LINK_NUM,
            Assignment.SIM_COL_PAX_B_TIME]].copy()
        next_trips[Passenger.PF_COL_LINK_NUM] = next_trips[Passenger.PF_COL_LINK_NUM] + 1
        next_trips.rename(columns={Assignment.SIM_COL_PAX_B_TIME:"new_trip_A_time"}, inplace=True)
        # Add it to passenger trips.  Now new_trip_A_time is set for trip links
        pathset_links_df = pd.merge(left  =pathset_links_df,
                                        right =next_trips,
                                        how   ="left",
                                        on    =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                Passenger.PF_COL_PATH_NUM,
                                                Passenger.PF_COL_LINK_NUM])
        pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_LINK_MODE]==PathSet.STATE_MODE_TRIP, Assignment.SIM_COL_PAX_A_TIME] = pathset_links_df["new_trip_A_time"]
        pathset_links_df.drop(["new_trip_A_time"], axis=1, inplace=True)

        pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_B_TIME] - pathset_links_df[Assignment.SIM_COL_PAX_A_TIME]

        #: todo: is there a more elegant way to take care of this?  some trips have times after midnight so they're the next day
        #: if the linktime > 22 hours then the trip time is probably off by a day, so it's right after midnight -- back it up
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME]/ np.timedelta64(1, 'h') > 22, Assignment.SIM_COL_PAX_B_TIME   ] = pathset_links_df[Assignment.SIM_COL_PAX_B_TIME] - np.timedelta64(24, 'h')
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME]/ np.timedelta64(1, 'h') > 22, Assignment.SIM_COL_PAX_LINK_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_B_TIME] - pathset_links_df[Assignment.SIM_COL_PAX_A_TIME]

        #: if the linktime < -22 hours then the trip time is probably off by a day, so it's right before midnight -- back it up
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME]/ np.timedelta64(-1, 'h') < -22, Assignment.SIM_COL_PAX_A_TIME   ] = pathset_links_df[Assignment.SIM_COL_PAX_A_TIME] - np.timedelta64(24, 'h')
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME]/ np.timedelta64(-1, 'h') < -22, Assignment.SIM_COL_PAX_LINK_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_B_TIME] - pathset_links_df[Assignment.SIM_COL_PAX_A_TIME]


        # new wait time
        pathset_links_df.loc[pd.notnull(pathset_links_df[Trip.TRIPS_COLUMN_TRIP_ID]), Assignment.SIM_COL_PAX_WAIT_TIME] = pathset_links_df[Assignment.SIM_COL_PAX_BOARD_TIME] - pathset_links_df[Assignment.SIM_COL_PAX_A_TIME]

        # invalid trips have negative wait time
        pathset_links_df[Assignment.SIM_COL_MISSED_XFER] = 0
        pathset_links_df.loc[pathset_links_df[Assignment.SIM_COL_PAX_WAIT_TIME]/ np.timedelta64(1,'m') < 0, Assignment.SIM_COL_MISSED_XFER] = 1

        # count how many are valid (sum of invalid = 0 for the trip list id + path)
        pathset_links_df_grouped = pathset_links_df.groupby([Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, # sort by this
                                                             Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                             Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                             Passenger.PF_COL_PATH_NUM]).aggregate({Assignment.SIM_COL_MISSED_XFER:"sum" })

        pathset_links_df_grouped.loc[pathset_links_df_grouped[Assignment.SIM_COL_MISSED_XFER]> 0, Assignment.SIM_COL_MISSED_XFER] = 1

        FastTripsLogger.info("          flag_missed_transfers found %d missed transfer trip legs for %d paths" % \
                             (pathset_links_df[Assignment.SIM_COL_MISSED_XFER].sum(),
                              pathset_links_df_grouped[Assignment.SIM_COL_MISSED_XFER].sum()))

        # add missed_xfer to pathset_paths_df (replacing if it was there already)
        if Assignment.SIM_COL_MISSED_XFER in list(pathset_paths_df.columns.values):
            pathset_paths_df.drop([Assignment.SIM_COL_MISSED_XFER], axis=1, inplace=True)

        pathset_paths_df = pd.merge(left  =pathset_paths_df,
                                        right =pathset_links_df_grouped.reset_index()[[Passenger.TRIP_LIST_COLUMN_PERSON_ID, Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID, Passenger.PF_COL_PATH_NUM, Assignment.SIM_COL_MISSED_XFER]],
                                        how   ="left")
        FastTripsLogger.debug("flag_missed_transfers() pathset_paths_df (%d):\n%s" % (len(pathset_paths_df), pathset_paths_df.head(30).to_string()))

        return (pathset_paths_df, pathset_links_df)

    @staticmethod
    def load_passengers_on_vehicles_with_cap(iteration, pathfinding_iteration, simulation_iteration,
                                             trips, pathset_paths_df, pathset_links_df, veh_loaded_df):
        """
        Check if we have boards on over-capacity vehicles.  Mark them and mark the boards.

        If :py:attr:`Assignment.CAPACITY_CONSTRAINT`, then bump off overcapacity passengers.

        The process is:

        1) Look at which vehicle links are over capacity, adding columns named :py:attr:`Trip.SIM_COL_VEH_OVERCAP`
           and py:attr:`Trip.SIM_COL_VEH_OVERCAP_FRAC` to *veh_loaded_df*

        2) Look at the stops where the first people board after we're at capacity (impossible boards) if any

        3) If :py:attr:`Assignment.BUMP_ONE_AT_A_TIME`, select the first such stop by arrival time
           Otherwise, select the first such stop for each vehicle trip

        4) Join these stops to pathset_links_df, so pathset_links_df now has column Assignment.SIM_COL_PAX_OVERCAP_FRAC

        5) If not :py:attr:`Assignment.CAPACITY_CONSTRAINT`, return (and drop the column named :py:attr:`Trip.SIM_COL_VEH_OVERCAP` from veh_loaded_df)

        6) Figure out which passenger trips are actually getting bumped.  Some people can get on at these stops, but not all, so let the first
           ones that arrive at the stop get on and filter to the ones we'll actually bump.  Update the column named :py:attr:`Assignmment.SIM_COL_PAX_BUMP_ITER`.
           If non-null, this represents the iteration the passenger got bumped.

        Return (chosen_paths_bumped, pathset_paths_df, pathset_links_df, veh_loaded_df)
        """
        # these are the relevant vehicle columns
        vehicle_trip_debug_columns = [ \
            Trip.TRIPS_COLUMN_ROUTE_ID,
            Trip.TRIPS_COLUMN_TRIP_ID,
            Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
            Trip.STOPTIMES_COLUMN_STOP_ID,
            Trip.VEHICLES_COLUMN_TOTAL_CAPACITY,
            Trip.SIM_COL_VEH_BOARDS,
            Trip.SIM_COL_VEH_ALIGHTS,
            Trip.SIM_COL_VEH_ONBOARD,
            Trip.SIM_COL_VEH_OVERCAP,
            Trip.SIM_COL_VEH_OVERCAP_FRAC
        ]
        # these are the relevant pathset links colums
        pax_links_debug_columns = [ \
            Passenger.TRIP_LIST_COLUMN_PERSON_ID,
            Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
            Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM,
            Passenger.PF_COL_PATH_NUM,
            Passenger.PF_COL_LINK_NUM,
            Passenger.PF_COL_ROUTE_ID,
            Passenger.PF_COL_TRIP_ID,
            Passenger.PF_COL_PAX_A_TIME,
            "A_id","A_id_num","A_seq",
            Assignment.SIM_COL_PAX_A_TIME,
            Assignment.SIM_COL_PAX_OVERCAP,
            Assignment.SIM_COL_PAX_OVERCAP_FRAC,
            Assignment.SIM_COL_PAX_BOARD_STATE,
            Assignment.SIM_COL_PAX_BUMP_ITER,
            Assignment.SIM_COL_PAX_CHOSEN,
        ]

        current_pf_iter  = 0.01*pathfinding_iteration + iteration
        current_sim_iter = "iter%.2f sim%d" % (current_pf_iter, simulation_iteration)

        # this will involve looping
        # no one is bumped yet
        bump_iter = 0
        pathset_links_df[Assignment.SIM_COL_PAX_OVERCAP_FRAC] = np.NaN

        if simulation_iteration==0:
            # For those we just found paths for, no one is bumped or going on overcap vehicles yet
            # pathset_paths_df.loc[pathset_paths_df[Passenger.PF_COL_PF_ITERATION]==current_pf_iter, Assignment.SIM_COL_PAX_BUMP_ITER   ] = np.NaN
            # pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_PF_ITERATION]==current_pf_iter, Assignment.SIM_COL_PAX_BUMP_ITER   ] = np.NaN
            # pathset_links_df.loc[pathset_links_df[Passenger.PF_COL_PF_ITERATION]==current_pf_iter, Assignment.SIM_COL_PAX_BOARD_STATE ] = np.NaN

            # anyone can be bumped, including those from previous pathfinding iters.  Otherwise, we wouldn't be able to ride to an earlier stop and bump them
            pathset_paths_df[ Assignment.SIM_COL_PAX_BUMP_ITER   ] = np.NaN
            pathset_links_df[ Assignment.SIM_COL_PAX_BUMP_ITER   ] = np.NaN
            pathset_links_df[ Assignment.SIM_COL_PAX_BOARD_STATE ] = np.NaN

        # make sure BOARD_STATE and CHOSEN are categorical
        pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN     ] = pd.Categorical( pathset_paths_df[Assignment.SIM_COL_PAX_CHOSEN     ], categories=Assignment.CHOSEN_CATEGORIES, ordered=True)
        pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN     ] = pd.Categorical( pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN     ], categories=Assignment.CHOSEN_CATEGORIES, ordered=True)
        pathset_links_df[Assignment.SIM_COL_PAX_BOARD_STATE] = pd.Categorical( pathset_links_df[Assignment.SIM_COL_PAX_BOARD_STATE], categories=Assignment.BOARD_STATE_CATEGORICAL)

        while True: # loop for capacity constraint

            FastTripsLogger.info("  Step 5.1 Put passengers on transit vehicles.")
            # Put passengers on vehicles, updating the vehicle's boards, alights, onboard, overcap, overcap_frac
            veh_loaded_df = Assignment.put_passengers_on_vehicles(pathset_links_df, veh_loaded_df)
            FastTripsLogger.debug("after putting passengers on vehicles, veh_loaded_df with onboard.head(30) = \n%s" %
                                  veh_loaded_df.loc[ veh_loaded_df[Trip.SIM_COL_VEH_ONBOARD]>0, vehicle_trip_debug_columns].head(30).to_string())

            if not Assignment.CAPACITY_CONSTRAINT:
                # We can't do anything about capacity so assume everyone boarded
                pathset_links_df.loc[ (pathset_links_df[Passenger.PF_COL_PF_ITERATION]==current_pf_iter)&
                                      (pathset_links_df[Passenger.PF_COL_TRIP_ID].notnull()),
                                       Assignment.SIM_COL_PAX_BOARD_STATE ] = "board_easy"
                break

            FastTripsLogger.info("  Step 5.2 Capacity constraints on transit vehicles.")
            if bump_iter == 0:
                FastTripsLogger.info("          Bumping one at a time? %s" % ("true" if Assignment.BUMP_ONE_AT_A_TIME else "false"))

            # This will update board time, alight time, overcap, overcap_frac
            pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_loaded_df)
            FastTripsLogger.debug("pathset_links_df.head(20)=\n%s" % pathset_links_df[pax_links_debug_columns].head(20).to_string())

            # make sure BOARD_STATE and CHOSEN are categorical
            pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN     ] = pd.Categorical( pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN     ], categories=Assignment.CHOSEN_CATEGORIES, ordered=True)
            pathset_links_df[Assignment.SIM_COL_PAX_BOARD_STATE] = pd.Categorical( pathset_links_df[Assignment.SIM_COL_PAX_BOARD_STATE], categories=Assignment.BOARD_STATE_CATEGORICAL)

            # CHOSEN: Everyone who can board easily, do so
            pathset_links_df.loc[ pathset_links_df[Passenger.PF_COL_ROUTE_ID].notnull() &                               # trip links only
                                  pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER].isnull() &                         # not already bumped
                                  (pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]>Assignment.CHOSEN_NOT_CHOSEN_YET)&   # chosen
                                  (pathset_links_df[Assignment.SIM_COL_PAX_OVERCAP]<0),                                 # can board
                                        Assignment.SIM_COL_PAX_BOARD_STATE ] = "board_easy"
            # CHOSEN:  Everyone who can squeeze in, do so
            pathset_links_df.loc[ pathset_links_df[Passenger.PF_COL_ROUTE_ID].notnull() &                               # trip links only
                                  pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER].isnull() &                         # not already bumped
                                  (pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]>Assignment.CHOSEN_NOT_CHOSEN_YET)&   # chosen
                                  (pathset_links_df[Assignment.SIM_COL_PAX_OVERCAP]==0),                                # can barely board
                                       Assignment.SIM_COL_PAX_BOARD_STATE ] = "boarded"
            # UNCHOSEN: paths that are overcap -- nope
            pathset_links_df.loc[ pathset_links_df[Passenger.PF_COL_ROUTE_ID].notnull() &                               # trip links only
                                  pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER].isnull() &                         # not already bumped
                                  (pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]==Assignment.CHOSEN_NOT_CHOSEN_YET)&  # unchosen
                                  (pathset_links_df[Assignment.SIM_COL_PAX_OVERCAP]>=0),                                # overcap
                                        Assignment.SIM_COL_PAX_BOARD_STATE ] = "bumped_unchosen"
            pathset_links_df.loc[ pathset_links_df[Passenger.PF_COL_ROUTE_ID].notnull() &                               # trip links only
                                  pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER].isnull() &                         # not already bumped
                                  (pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]==Assignment.CHOSEN_NOT_CHOSEN_YET)&  # unchosen
                                  (pathset_links_df[Assignment.SIM_COL_PAX_OVERCAP]>=0),                                # overcap
                                        Assignment.SIM_COL_PAX_BUMP_ITER ] = bump_iter

            # For those trying to board overcap, choose the winners and losers
            # These are trips/stops over capacity
            overcap_df = veh_loaded_df.loc[veh_loaded_df[Trip.SIM_COL_VEH_OVERCAP] > 0]
            FastTripsLogger.debug("load_passengers_on_vehicles_with_cap() %d vehicle trip/stops over capacity: (showing head)\n%s" % \
                                  (len(overcap_df), overcap_df[vehicle_trip_debug_columns].head().to_string()))

            # If none, we're done
            if len(overcap_df) == 0:
                FastTripsLogger.info("          No over-capacity vehicles")
                break

            # 2) Look at the trip-stops where the *first people* board after we're at capacity (impossible boards) if any
            bump_stops_df = overcap_df.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID]).aggregate('first').reset_index()
            FastTripsLogger.debug("load_passengers_on_vehicles_with_cap() bump_stops_df iter=%d pf_iter=%d sim_iter=%d bump_iter=%d (%d rows, showing head):\n%s" %
                                  (iteration, pathfinding_iteration, simulation_iteration, bump_iter,
                                   len(bump_stops_df), bump_stops_df[vehicle_trip_debug_columns].head().to_string()))

            if Assignment.BUMP_ONE_AT_A_TIME:
                bump_stops_df.sort_values(by=[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME], inplace=True)
                bump_stops_df = bump_stops_df.iloc[:1]

            FastTripsLogger.info("          Need to bump %d passengers from %d trip-stops" % (bump_stops_df.overcap.sum(), len(bump_stops_df)))
            # debug -- see the whole trip
            if True:
                FastTripsLogger.debug("load_passengers_on_vehicles_with_cap() Trips with bump stops:\n%s\n" % \
                    pd.merge(
                        left=veh_loaded_df[vehicle_trip_debug_columns],
                        right=bump_stops_df[[Trip.STOPTIMES_COLUMN_TRIP_ID]],
                        how='inner').to_string())

            # make sure CHOSEN is categorical
            pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN     ] = pd.Categorical( pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN     ], categories=Assignment.CHOSEN_CATEGORIES, ordered=True)

            # join CHOSEN pathset links to bump_stops_df; now passenger links boarding at a bump stop will have Trip.STOPTIMES_COLUMN_STOP_SEQUENCE set
            bumpstop_boards = pd.merge(left    =pathset_links_df.loc[ pathset_links_df[Passenger.PF_COL_ROUTE_ID].notnull() &                               # trip links only
                                                                          pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER].isnull() &                         # not already bumped
                                                                          (pathset_links_df[Assignment.SIM_COL_PAX_CHOSEN]>Assignment.CHOSEN_NOT_CHOSEN_YET) ], # chosen
                                           left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID, "A_seq"],
                                           right   =bump_stops_df[[Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]],
                                           right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                           how     ="left")
            # bump candidates: boarding at bump stops, chosen paths
            bumpstop_boards = bumpstop_boards.loc[ bumpstop_boards[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE].notnull(), # board at bump_stops_df stop
                                                   pax_links_debug_columns].copy()

            # bump off later arrivals, later trip_list_num
            bumpstop_boards.sort_values(by=[ \
                Assignment.SIM_COL_PAX_A_TIME, # I think this is correct
                Trip.STOPTIMES_COLUMN_TRIP_ID,
                "A_seq",
                Passenger.PF_COL_PAX_A_TIME,
                Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM],
                ascending=[True, True, True, False, False], inplace=True)
            bumpstop_boards.reset_index(drop=True, inplace=True)

            # For each trip_id, stop_seq, stop_id, we want the first *overcap* rows
            # group to trip_id, stop_seq, stop_id and count off
            bpb_count = bumpstop_boards.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID,"A_seq","A_id_num"]).cumcount()
            bpb_count.name = 'bump_index'
            # Add the bump index to our passenger-paths/stops
            bumpstop_boards = pd.concat([bumpstop_boards, bpb_count], axis=1)

            # bump or board them
            bumpstop_boards[ Assignment.SIM_COL_PAX_BOARD_STATE ] = pd.Categorical(["boarded"]*len(bumpstop_boards), categories=Assignment.BOARD_STATE_CATEGORICAL)
            bumpstop_boards.loc[ bumpstop_boards["bump_index"] < bumpstop_boards[Trip.SIM_COL_VEH_OVERCAP], Assignment.SIM_COL_PAX_BOARD_STATE ] = "bumped"  # these folks got bumped
            bumpstop_boards.loc[ bumpstop_boards["bump_index"] < bumpstop_boards[Trip.SIM_COL_VEH_OVERCAP], Assignment.SIM_COL_PAX_BUMP_ITER   ] = bump_iter  # these folks got bumped

            FastTripsLogger.debug("load_passengers_on_vehicles_with_cap() bumpstop_boards (%d rows, showing head):\n%s" % \
                                  (len(bumpstop_boards), bumpstop_boards.head(50).to_string()))

            # filter to unique passengers/paths who got bumped
            bump_paths = bumpstop_boards.loc[ bumpstop_boards[ Assignment.SIM_COL_PAX_BOARD_STATE ] == "bumped",
                [Passenger.TRIP_LIST_COLUMN_PERSON_ID, Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM, Passenger.PF_COL_PATH_NUM]].drop_duplicates()
            chosen_paths_bumped = len(bump_paths)

            # figure when the wait time starts for the bump stops
            new_bump_wait = bumpstop_boards[[Trip.STOPTIMES_COLUMN_TRIP_ID, "A_seq", "A_id_num", Passenger.PF_COL_PAX_A_TIME]].groupby( \
                                            [Trip.STOPTIMES_COLUMN_TRIP_ID, "A_seq","A_id_num"]).first().reset_index(drop=False)
            new_bump_wait.rename(columns={"A_seq"   :Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                          "A_id_num":Trip.STOPTIMES_COLUMN_STOP_ID_NUM}, inplace=True)
            # need trip id num
            new_bump_wait = trips.add_numeric_trip_id(new_bump_wait, Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_TRIP_ID_NUM)
            FastTripsLogger.debug("new_bump_wait (%d rows, showing head):\n%s" % (len(new_bump_wait), new_bump_wait.head().to_string()))

             # incorporate it into the bump wait df
            if type(Assignment.bump_wait_df) == type(None):
                Assignment.bump_wait_df = new_bump_wait
            else:
                Assignment.bump_wait_df = pd.concat([Assignment.bump_wait_df, new_bump_wait], axis=0)

                FastTripsLogger.debug("load_passengers_on_vehicles_with_cap() bump_wait_df (%d rows, showing head):\n%s" %
                    (len(Assignment.bump_wait_df), Assignment.bump_wait_df.head().to_string()))

                Assignment.bump_wait_df.drop_duplicates(subset=[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                                Trip.STOPTIMES_COLUMN_STOP_SEQUENCE], inplace=True)

            # finally, incorporate the board state and bump_iter to the full pathset_links_df
            pathset_links_df = pd.merge(left   =pathset_links_df,
                                            right  =bumpstop_boards[[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                                     Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                                     Passenger.PF_COL_PATH_NUM,
                                                                     Passenger.PF_COL_LINK_NUM,
                                                                     Assignment.SIM_COL_PAX_BOARD_STATE,
                                                                     Assignment.SIM_COL_PAX_BUMP_ITER]],
                                            on     =[Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                     Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                     Passenger.PF_COL_PATH_NUM,
                                                     Passenger.PF_COL_LINK_NUM],
                                            how    ="left",
                                            suffixes=[""," bb"],
                                            indicator=True)
            pathset_links_df.loc[ pathset_links_df["_merge"]=="both", Assignment.SIM_COL_PAX_BOARD_STATE ] = pathset_links_df["%s bb" % Assignment.SIM_COL_PAX_BOARD_STATE]
            pathset_links_df.loc[ pathset_links_df["_merge"]=="both", Assignment.SIM_COL_PAX_BUMP_ITER   ] = pathset_links_df["%s bb" % Assignment.SIM_COL_PAX_BUMP_ITER  ]
            pathset_links_df.drop(["_merge","%s bb" % Assignment.SIM_COL_PAX_BOARD_STATE, "%s bb" % Assignment.SIM_COL_PAX_BUMP_ITER], axis=1, inplace=True)
            FastTripsLogger.debug(pathset_links_df[pax_links_debug_columns].head())

            # bump the whole path
            bump_paths_df = pathset_links_df.loc[ pathset_links_df[Assignment.SIM_COL_PAX_BUMP_ITER]==bump_iter,
                                                 [Passenger.TRIP_LIST_COLUMN_PERSON_ID,
                                                  Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID,
                                                  Passenger.PF_COL_PATH_NUM]].drop_duplicates()
            pathset_paths_df = pd.merge(left     =pathset_paths_df,
                                            right    =bump_paths_df,
                                            how      ="left",
                                            indicator=True)
            pathset_paths_df.loc[ pathset_paths_df["_merge"]=="both", Assignment.SIM_COL_PAX_BUMP_ITER ] = bump_iter
            pathset_paths_df.drop(["_merge"], axis=1, inplace=True)

            # communicate back to other links in the same path too
            pathset_links_df = pd.merge(left     =pathset_links_df,
                                            right    =bump_paths_df,
                                            how      ="left",
                                            indicator=True)
            pathset_links_df.loc[ pathset_links_df["_merge"]=="both", Assignment.SIM_COL_PAX_BUMP_ITER ] = bump_iter
            pathset_links_df.loc[ (pathset_links_df["_merge"]=="both")&
                                  (pathset_links_df[Assignment.SIM_COL_PAX_BOARD_STATE].isnull()|
                                   (pathset_links_df[Assignment.SIM_COL_PAX_BOARD_STATE]=="boarded")|
                                   (pathset_links_df[Assignment.SIM_COL_PAX_BOARD_STATE]=="board_easy"))&
                                  pathset_links_df[Passenger.PF_COL_ROUTE_ID].notnull(),
                                  Assignment.SIM_COL_PAX_BOARD_STATE ] = "bumped_othertrip"
            pathset_links_df.drop(["_merge"], axis=1, inplace=True)

            FastTripsLogger.info("        -> completed loop bump_iter %d and bumped %d chosen paths" % (bump_iter, chosen_paths_bumped))

            if chosen_paths_bumped == 0:
                break

            bump_iter += 1

        if type(Assignment.bump_wait_df) == pd.DataFrame and len(Assignment.bump_wait_df) > 0:
            Assignment.bump_wait_df[Passenger.PF_COL_PAX_A_TIME_MIN] = \
                Assignment.bump_wait_df[Passenger.PF_COL_PAX_A_TIME].map(lambda x: (60.0*x.hour) + x.minute + (x.second/60.0))

        if type(Assignment.bump_wait_df) == pd.DataFrame and len(Assignment.bump_wait_df) > 0:
            FastTripsLogger.debug("Bump_wait_df:\n%s" % Assignment.bump_wait_df.to_string())

        return (pathset_paths_df, pathset_links_df, veh_loaded_df)

    @staticmethod
    def choose_paths_without_simulation(FT, output_dir, iteration, pathfinding_iteration, pathset_paths_df, pathset_links_df, veh_trips_df):
        """
        Given a pathset for each passernger, choose a path (if relevant).  That's it.

        Returns (valid_linked_trips, pathset_paths_df, pathset_links_df)
        """
        simulation_iteration   = 0
        num_passengers_arrived = 0
        ######################################################################################################
        FastTripsLogger.info("  Step 1. Find out board/alight times for all pathset links from vehicle times")

        # could do this just to chosen path links but let's do this to the whole pathset
        pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_trips_df)

        # instead of flag_missed_transfers(), set these to pathfinding results
        pathset_links_df[Assignment.SIM_COL_PAX_ALIGHT_DELAY_MIN] = 0
        pathset_links_df[Assignment.SIM_COL_PAX_A_TIME          ] = pathset_links_df[Passenger.PF_COL_PAX_A_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_B_TIME          ] = pathset_links_df[Passenger.PF_COL_PAX_B_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_LINK_TIME       ] = pathset_links_df[Passenger.PF_COL_LINK_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_WAIT_TIME       ] = pathset_links_df[Passenger.PF_COL_WAIT_TIME]
        pathset_links_df[Assignment.SIM_COL_PAX_MISSED_XFER     ] = 0

        ######################################################################################################
        FastTripsLogger.info("  Step 2. Calculate costs and probabilities for all pathset paths")
        (pathset_paths_df, pathset_links_df) = PathSet.calculate_cost(
            Assignment.STOCH_DISPERSION, pathset_paths_df, pathset_links_df, FT.veh_trips_df,
            FT.passengers.trip_list_df, FT.routes, FT.tazs, FT.transfers, stops=FT.stops,
            reset_bump_iter=simulation_iteration==0)

        ######################################################################################################
        FastTripsLogger.info("  Step 3. Choose a path for each passenger from their pathset")

        # Choose path for each passenger -- pathset_paths_df and pathset_links_df will now have
        # SIM_COL_PAX_CHOSEN >=0 for chosen paths/path links
        (num_passengers_arrived, num_chosen, pathset_paths_df, pathset_links_df) = Passenger.choose_paths(
            True,  # choose for everyone
            iteration, pathfinding_iteration, simulation_iteration,
            pathset_paths_df, pathset_links_df)

        # Write the pathsets
        Passenger.write_paths(output_dir, iteration, pathfinding_iteration, simulation_iteration, pathset_paths_df, False, Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, not Assignment.DEBUG_OUTPUT_COLUMNS)
        Passenger.write_paths(output_dir, iteration, pathfinding_iteration, simulation_iteration, pathset_links_df, True,  Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, not Assignment.DEBUG_OUTPUT_COLUMNS)

        # write the final chosen paths for this iteration
        chosen_links_df = Passenger.get_chosen_links(pathset_links_df)
        chosen_links_df["iteration"] = iteration
        Util.write_dataframe(chosen_links_df, "chosen_links_df", os.path.join(output_dir, "chosenpaths_links.csv"), append=(iteration>1),
                             drop_debug_columns      =not Assignment.DEBUG_OUTPUT_COLUMNS,
                             drop_pathfinding_columns=not Assignment.DEBUG_OUTPUT_COLUMNS)
        chosen_links_df.drop(["iteration"], axis=1, inplace=True)

        chosen_paths_df = Passenger.get_chosen_links(pathset_paths_df)
        chosen_paths_df["iteration"] = iteration
        Util.write_dataframe(chosen_paths_df, "chosen_paths_df", os.path.join(output_dir, "chosenpaths_paths.csv"), append=(iteration>1),
                             drop_debug_columns      =not Assignment.DEBUG_OUTPUT_COLUMNS,
                             drop_pathfinding_columns=not Assignment.DEBUG_OUTPUT_COLUMNS)
        chosen_paths_df.drop(["iteration"], axis=1, inplace=True)

        return (num_passengers_arrived, pathset_paths_df, pathset_links_df)

    @staticmethod
    def simulate(FT, output_dir, iteration, pathfinding_iteration, pathset_paths_df, pathset_links_df, veh_trips_df):
        """
        Given a pathset for each passenger, choose a path (if relevant) and then
        actually assign the passengers trips to the vehicles.

        Returns (valid_linked_trips, pathset_paths_df, pathset_links_df, veh_loaded_df)
        """
        simulation_iteration   = 0
        num_passengers_arrived = 0 # will get returned from choose_paths

        while True:
            FT.performance.record_step_start(iteration, pathfinding_iteration, simulation_iteration, "simulation iteration")
            FastTripsLogger.info("Simulation Iteration %d" % simulation_iteration)
            # for trace_tuple in Assignment.TRACE_PERSON_IDS:
            #     FastTripsLogger.debug("Initial pathset_links_df for %s\n%s" % \
            #        (str(trace_pax), pathset_links_df.loc[pathset_links_df.person_id==trace_pax].to_string()))
            #     FastTripsLogger.debug("Initial pathset_paths_df for %s\n%s" % \
            #        (str(trace_pax), pathset_paths_df.loc[pathset_paths_df.person_id==trace_pax].to_string()))

            ######################################################################################################
            FastTripsLogger.info("  Step 1. Find out board/alight times for all pathset links from vehicle times")

            # could do this just to chosen path links but let's do this to the whole pathset
            pathset_links_df = Assignment.find_passenger_vehicle_times(pathset_links_df, veh_trips_df)

            ######################################################################################################
            FastTripsLogger.info("  Step 2. Flag missed transfer links and paths in the pathsets")
            (pathset_paths_df, pathset_links_df) = Assignment.flag_missed_transfers(pathset_paths_df, pathset_links_df)

            ######################################################################################################
            FastTripsLogger.info("  Step 3. Calculate costs and probabilities for all pathset paths")
            (pathset_paths_df, pathset_links_df) = PathSet.calculate_cost(
                Assignment.STOCH_DISPERSION, pathset_paths_df, pathset_links_df, veh_trips_df,
                FT.passengers.trip_list_df, FT.routes, FT.tazs, FT.transfers, stops=FT.stops,
                reset_bump_iter=simulation_iteration == 0)

            ######################################################################################################
            FastTripsLogger.info("  Step 4. Choose a path for each passenger from their pathset")

            # Choose path for each passenger -- pathset_paths_df and pathset_links_df will now have
            # SIM_COL_PAX_CHOSEN >=0 for chosen paths/path links
            (num_passengers_arrived, num_chosen, pathset_paths_df, pathset_links_df) = Passenger.choose_paths(
                Assignment.PATHFINDING_EVERYONE and simulation_iteration==0,  # choose for everyone if we just re-found all paths
                iteration, pathfinding_iteration, simulation_iteration,
                pathset_paths_df, pathset_links_df)

            ######################################################################################################
            FastTripsLogger.info("  Step 5. Put passenger paths on transit vehicles to get vehicle boards/alights/load and assess capacity constraints")

            (pathset_paths_df, pathset_links_df, veh_trips_df) = Assignment.load_passengers_on_vehicles_with_cap(
                iteration, pathfinding_iteration, simulation_iteration,
                FT.trips, pathset_paths_df, pathset_links_df, veh_trips_df)

            ######################################################################################################
            FastTripsLogger.info("  Step 6. Update dwell and travel times for transit vehicles")
            # update the trip times -- accel/decel rates + stops affect travel times, and boards/alights affect dwell times
            veh_trips_df   = Trip.update_trip_times(veh_trips_df, Assignment.MSA_RESULTS)

            ######################################################################################################
            if Assignment.OUTPUT_PATHSET_PER_SIM_ITER:
                FastTripsLogger.info("  Step 7. Write pathsets (paths and links)")
                Passenger.write_paths(output_dir, iteration, pathfinding_iteration, simulation_iteration, pathset_paths_df, False,
                                      Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, not Assignment.DEBUG_OUTPUT_COLUMNS)
                Passenger.write_paths(output_dir, iteration, pathfinding_iteration, simulation_iteration, pathset_links_df, True,
                                      Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, not Assignment.DEBUG_OUTPUT_COLUMNS)
                # and vehicle trips
                Assignment.write_vehicle_trips(output_dir, iteration, pathfinding_iteration, simulation_iteration, veh_trips_df)


            FT.performance.record_step_end(iteration, pathfinding_iteration, simulation_iteration)
            simulation_iteration += 1

            if num_chosen <= 0:
                FastTripsLogger.info("  No more path choices to make => Ending simulation loop")
                break

            if simulation_iteration > Assignment.MAX_SIMULATION_ITERS:
                FastTripsLogger.info("  Maximum simulation iterations reached (%d) => Ending simulation loop" % Assignment.MAX_SIMULATION_ITERS)
                break

        FT.performance.record_step_start(iteration, pathfinding_iteration, simulation_iteration, "output_per_simulation")

        # Write the pathsets (if we haven't been already)
        if Assignment.OUTPUT_PATHSET_PER_SIM_ITER == False:
            Passenger.write_paths(output_dir, iteration, pathfinding_iteration, simulation_iteration, pathset_paths_df, False,
                                  Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, not Assignment.DEBUG_OUTPUT_COLUMNS)
            Passenger.write_paths(output_dir, iteration, pathfinding_iteration, simulation_iteration, pathset_links_df, True,
                                  Assignment.OUTPUT_PATHSET_PER_SIM_ITER, not Assignment.DEBUG_OUTPUT_COLUMNS, not Assignment.DEBUG_OUTPUT_COLUMNS)

        # write the final chosen paths for this iteration
        chosen_links_df = Passenger.get_chosen_links(pathset_links_df)
        chosen_links_df["iteration"]             = iteration
        chosen_links_df["pathfinding_iteration"] = pathfinding_iteration
        Util.write_dataframe(chosen_links_df, "chosen_links_df", os.path.join(output_dir, "chosenpaths_links.csv"), append=((iteration>1) or (pathfinding_iteration>1)),
                             drop_debug_columns      =not Assignment.DEBUG_OUTPUT_COLUMNS,
                             drop_pathfinding_columns=not Assignment.DEBUG_OUTPUT_COLUMNS)
        chosen_links_df.drop(["iteration", "pathfinding_iteration"], axis=1, inplace=True)

        chosen_paths_df = Passenger.get_chosen_links(pathset_paths_df)

        chosen_paths_df["iteration"            ] = iteration
        chosen_paths_df["pathfinding_iteration"] = pathfinding_iteration
        Util.write_dataframe(chosen_paths_df, "chosen_paths_df", os.path.join(output_dir, "chosenpaths_paths.csv"), append=((iteration>1) or (pathfinding_iteration>1)),
                             drop_debug_columns      =not Assignment.DEBUG_OUTPUT_COLUMNS,
                             drop_pathfinding_columns=not Assignment.DEBUG_OUTPUT_COLUMNS)
        chosen_paths_df.drop(["iteration", "pathfinding_iteration"], axis=1, inplace=True)

        FT.performance.record_step_end(iteration, pathfinding_iteration, simulation_iteration)

        return (num_passengers_arrived, pathset_paths_df, pathset_links_df, veh_trips_df)


def find_trip_based_paths_process_worker(iteration, pathfinding_iteration, worker_num, input_network_dir, input_demand_dir, run_config, func_file,
                                         output_dir, todo_pathset_queue, done_queue, hyperpath, bump_wait_df, stop_times_df):
    """
    Process worker function.  Processes all the paths in queue.

    todo_queue has (passenger_id, path object)
    """
    worker_str = "_worker%02d" % worker_num

    from .FastTrips import FastTrips
    setupLogging(infoLogFilename  = None,
                 debugLogFilename = os.path.join(output_dir, FastTrips.DEBUG_LOG % worker_str),
                 logToConsole     = False,
                 append           = False if ((iteration==1) and (pathfinding_iteration==1)) else True)
    FastTripsLogger.info("Iteration %d Pathfinding Iteration %d Worker %2d starting" % (iteration, pathfinding_iteration, worker_num))

    # the child process doesn't have these set so read them
    Assignment.CONFIGURATION_FILE           = run_config
    Assignment.CONFIGURATION_FUNCTIONS_FILE = func_file
    Assignment.read_functions(func_file)
    Assignment.read_configuration(run_config)

    # this passes those read parameters and the stop times to the C++ extension
    Assignment.initialize_fasttrips_extension(worker_num, output_dir, stop_times_df)

    # the extension has it now, so we're done
    stop_times_df = None

    if iteration > 1:
        Assignment.set_fasttrips_bump_wait(bump_wait_df)

    while True:
        # go through my queue -- check if we're done
        todo = todo_pathset_queue.get()
        if todo == 'DONE':
            done_queue.put( (worker_num, 'DONE') )
            FastTripsLogger.debug("Received DONE from the todo_pathset_queue")
            return

        # do the work
        pathset = todo

        FastTripsLogger.info("Processing person %20s trip %20s" % (pathset.person_id, pathset.person_trip_id))
        # communicate it to the parent
        done_queue.put( (worker_num, "STARTING", pathset.person_id, pathset.person_trip_id ))

        trace_person = False
        if (pathset.person_id, pathset.person_trip_id) in Assignment.TRACE_IDS:
            FastTripsLogger.debug("Tracing assignment of person %s trip %s" % (pathset.person_id, pathset.person_trip_id))
            trace_person = True

        try:
            (pathdict, perf_dict) = Assignment.find_trip_based_pathset(iteration, pathfinding_iteration, pathset, hyperpath, trace=trace_person)
            done_queue.put( (worker_num, "COMPLETED", pathset.trip_list_id_num, pathdict, perf_dict) )
        except:
            FastTripsLogger.exception("Exception")
            # call it a day
            done_queue.put( (worker_num, "EXCEPTION", str(sys.exc_info()) ) )
            return
