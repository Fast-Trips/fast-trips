__copyright__ = "Copyright 2016 Contributing Entities"
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
import datetime
import os

import pandas as pd

from .Passenger import Passenger
from .Util      import Util


class Performance:
    """
    Performance class.  Keeps track of performance information
    (time spent, number of labeling iterations, etc) related to pathfinding
    in Fast-Trips and for the the bigger loops.
    """
    #: Performance column: Iteration
    PERFORMANCE_PF_COL_ITERATION              = "iteration"
    #: Performance column: Pathfinding Iteration
    PERFORMANCE_PF_COL_PATHFINDING_ITERATION  = "pathfinding_iteration"
    #: Performance column: Person ID
    PERFORMANCE_PF_COL_PERSON_ID              = Passenger.TRIP_LIST_COLUMN_PERSON_ID
    #: Performance column: Person Trip ID
    PERFORMANCE_PF_COL_PERSON_TRIP_ID         = Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID
    #: Performance column: Process number
    PERFORMANCE_PF_COL_PROCESS_NUM            = "process number"
    #: Performance column: Pathfinding status
    PERFORMANCE_PF_COL_PATHFINDING_STATUS     = "pathfinding_status"
    #: Performance column: Number of label iterations
    PERFORMANCE_PF_COL_LABEL_ITERATIONS       = "label iterations"
    #: Performance column: Number of labeled stops
    PERFORMANCE_PF_COL_NUM_LABELED_STOPS      = "num labeled stops"
    #: Performance column: Maximum number of times a stop was processed
    PERFORMANCE_PF_COL_MAX_STOP_PROCESS_COUNT = "max stop process count"
    #: Performance column: Time spent labeling (timedelta)
    PERFORMANCE_PF_COL_TIME_LABELING          = "time labeling"
    #: Performance column: Time spent labeling (milliseconds)
    PERFORMANCE_PF_COL_TIME_LABELING_MS       = "time labeling milliseconds"
    #: Performance column: Time spent enumerating (timedelta)
    PERFORMANCE_PF_COL_TIME_ENUMERATING       = "time enumerating"
    #: Performance column: Time spent enumerating (milliseconds)
    PERFORMANCE_PF_COL_TIME_ENUMERATING_MS    = "time enumerating milliseconds"
    #: Performance column: Traced, since this affects performance
    PERFORMANCE_PF_COL_TRACED                 = "traced"
    #: Performance column: Working set in memory, in bytes
    PERFORMANCE_PF_COL_WORKING_SET_BYTES      = "working set bytes"
    #: Performance column: Private usage in memory, in bytes
    PERFORMANCE_PF_COL_PRIVATE_USAGE_BYTES    = "private usage bytes"
    #: Performance column: Timestamp of memory query, in a datetime.datetime
    PERFORMANCE_PF_COL_MEM_TIMESTAMP          = "mem_timestamp"

    #: File to write performance results
    OUTPUT_PERFORMANCE_PF_FILE                = 'ft_output_performance_pathfinding.csv'

    #: For general performance (not pathfinding)
    #: Performance column: Step name (e.g. read inputs). String.
    PERFORMANCE_COL_STEP_NAME                 = "step_name"
    #: Performance column: Iteration. Integer.
    PERFORMANCE_COL_ITERATION                 = PERFORMANCE_PF_COL_ITERATION
    #: Performance column: Pathfinding Iteration. Integer.
    PERFORMANCE_COL_PATHFINDING_ITERATION     = PERFORMANCE_PF_COL_PATHFINDING_ITERATION
    #: Performance column: Simulation Iteration. Integer.
    PERFORMANCE_COL_SIMULATION_ITERATION      = "simulation_iteration"
    #: Performance column: step start time
    PERFORMANCE_COL_START_TIME                = "start_time"
    PERFORMANCE_COL_END_TIME                  = "end_time"
    PERFORMANCE_COL_STEP_DURATION             = "step_duration"
    PERFORMANCE_COL_START_MEM_MB              = "start_mem_MB"
    PERFORMANCE_COL_END_MEM_MB                = "end_mem_MB"

    #: File to write performance results
    OUTPUT_PERFORMANCE_FILE                = 'ft_output_performance.csv'

    def __init__(self):
        """
        Constructor.  Initialize empty dataframe for performance info.
        """
        self.performance_pf_dict = {
            Performance.PERFORMANCE_PF_COL_ITERATION                :[],
            Performance.PERFORMANCE_PF_COL_PATHFINDING_ITERATION    :[],
            Performance.PERFORMANCE_PF_COL_PERSON_ID                :[],
            Performance.PERFORMANCE_PF_COL_PERSON_TRIP_ID           :[],
            Performance.PERFORMANCE_PF_COL_PROCESS_NUM              :[],
            Performance.PERFORMANCE_PF_COL_PATHFINDING_STATUS       :[],
            Performance.PERFORMANCE_PF_COL_NUM_LABELED_STOPS        :[],
            Performance.PERFORMANCE_PF_COL_TRACED                   :[],
            Performance.PERFORMANCE_PF_COL_LABEL_ITERATIONS         :[],
            Performance.PERFORMANCE_PF_COL_MAX_STOP_PROCESS_COUNT   :[],
            Performance.PERFORMANCE_PF_COL_TIME_LABELING            :[],
            Performance.PERFORMANCE_PF_COL_TIME_LABELING_MS         :[],
            Performance.PERFORMANCE_PF_COL_TIME_ENUMERATING         :[],
            Performance.PERFORMANCE_PF_COL_TIME_ENUMERATING_MS      :[],
            Performance.PERFORMANCE_PF_COL_WORKING_SET_BYTES        :[],
            Performance.PERFORMANCE_PF_COL_PRIVATE_USAGE_BYTES      :[],
            Performance.PERFORMANCE_PF_COL_MEM_TIMESTAMP            :[]
        }

        # maps PERFORMANCE_COLUMN* to arrays of values
        self.step_record_dict = {
            Performance.PERFORMANCE_COL_STEP_NAME                   :[],
            Performance.PERFORMANCE_COL_ITERATION                   :[],
            Performance.PERFORMANCE_COL_PATHFINDING_ITERATION       :[],
            Performance.PERFORMANCE_COL_SIMULATION_ITERATION        :[],
            Performance.PERFORMANCE_COL_START_TIME                  :[],
            Performance.PERFORMANCE_COL_END_TIME                    :[],
            # Performance.PERFORMANCE_COL_STEP_DURATION             :[],  # do this at the end
            Performance.PERFORMANCE_COL_START_MEM_MB                :[],
            Performance.PERFORMANCE_COL_END_MEM_MB                  :[]
        }

        # will map (iteration, pathfinding_iteration, simulation_iteration) => (step_name, start time (a datetime.datetime), starting mem usage in bytes)
        self.steps = {}

    def add_info(self, iteration, pathfinding_iteration, person_id, person_trip_id,  perf_dict):
        """
        Add this row to the performance dict of arrays.
        Assumes time values are in milliseconds.
        """
        self.performance_pf_dict[Performance.PERFORMANCE_PF_COL_ITERATION].append(iteration)
        self.performance_pf_dict[Performance.PERFORMANCE_PF_COL_PATHFINDING_ITERATION].append(pathfinding_iteration)
        self.performance_pf_dict[Performance.PERFORMANCE_PF_COL_PERSON_ID].append(person_id)
        self.performance_pf_dict[Performance.PERFORMANCE_PF_COL_PERSON_TRIP_ID].append(person_trip_id)

        for key in [Performance.PERFORMANCE_PF_COL_PROCESS_NUM,
                    Performance.PERFORMANCE_PF_COL_PATHFINDING_STATUS,
                    Performance.PERFORMANCE_PF_COL_LABEL_ITERATIONS,
                    Performance.PERFORMANCE_PF_COL_NUM_LABELED_STOPS,
                    Performance.PERFORMANCE_PF_COL_TRACED,
                    Performance.PERFORMANCE_PF_COL_MAX_STOP_PROCESS_COUNT,
                    Performance.PERFORMANCE_PF_COL_TIME_LABELING_MS,
                    Performance.PERFORMANCE_PF_COL_TIME_ENUMERATING_MS,
                    Performance.PERFORMANCE_PF_COL_WORKING_SET_BYTES,
                    Performance.PERFORMANCE_PF_COL_PRIVATE_USAGE_BYTES,
                    Performance.PERFORMANCE_PF_COL_MEM_TIMESTAMP]:
            self.performance_pf_dict[key].append(perf_dict[key])

        # convert milliseconds time to timedeltas
        self.performance_pf_dict[Performance.PERFORMANCE_PF_COL_TIME_LABELING   ].append(datetime.timedelta(milliseconds=perf_dict[Performance.PERFORMANCE_PF_COL_TIME_LABELING_MS   ]))
        self.performance_pf_dict[Performance.PERFORMANCE_PF_COL_TIME_ENUMERATING].append(datetime.timedelta(milliseconds=perf_dict[Performance.PERFORMANCE_PF_COL_TIME_ENUMERATING_MS]))

    def record_step_start(self, iteration, pathfinding_iteration, simulation_iteration, step_name):
        """
        Records the step start.
        If there was previously a step at this level, then ends that and saves the duration of that step.
        """
        key     = (iteration, pathfinding_iteration, simulation_iteration)
        now     = datetime.datetime.now()
        mem_use = Util.get_process_mem_use_bytes()/1000000.0

        # if we were already doing something, record it
        if key in self.steps:
            self.record_step_end(iteration, pathfinding_iteration, simulation_iteration)

        # save the current step state
        self.steps[key] = (step_name, now, mem_use)

    def record_step_end(self, iteration, pathfinding_iteration, simulation_iteration):
        """
        Explicitly ends whatever step was happening at this level.
        """
        key       = (iteration, pathfinding_iteration, simulation_iteration)
        now       = datetime.datetime.now()
        mem_use   = Util.get_process_mem_use_bytes()/1000000.0

        if key not in self.steps:
            return

        prev_step = self.steps[key]

        self.step_record_dict[Performance.PERFORMANCE_COL_STEP_NAME            ].append(prev_step[0])
        self.step_record_dict[Performance.PERFORMANCE_COL_ITERATION            ].append(iteration)
        self.step_record_dict[Performance.PERFORMANCE_COL_PATHFINDING_ITERATION].append(pathfinding_iteration)
        self.step_record_dict[Performance.PERFORMANCE_COL_SIMULATION_ITERATION ].append(simulation_iteration)
        self.step_record_dict[Performance.PERFORMANCE_COL_START_TIME           ].append(prev_step[1])
        self.step_record_dict[Performance.PERFORMANCE_COL_END_TIME             ].append(now)
        self.step_record_dict[Performance.PERFORMANCE_COL_START_MEM_MB         ].append(prev_step[2])
        self.step_record_dict[Performance.PERFORMANCE_COL_END_MEM_MB           ].append(mem_use)

        del self.steps[key]

    def write_pathfinding(self, output_dir, append):
        """
        Writes the pathfinding results to OUTPUT_PERFORMANCE_PF_FILE as a csv.
        """
        performance_df = pd.DataFrame.from_dict(self.performance_pf_dict)

        Util.write_dataframe(performance_df, "performance_df", os.path.join(output_dir, Performance.OUTPUT_PERFORMANCE_PF_FILE), append=append)

        # reset dict to blank
        for key in self.performance_pf_dict.keys():
            self.performance_pf_dict[key] = []

    def write(self, output_dir):
        """
        Writes the results to OUTPUT_PERFORMANCE_FILE as a csv.
        """
        performance_df = pd.DataFrame.from_dict(self.step_record_dict)

        performance_df[Performance.PERFORMANCE_COL_STEP_DURATION] = performance_df[Performance.PERFORMANCE_COL_END_TIME] - performance_df[Performance.PERFORMANCE_COL_START_TIME]

        Util.write_dataframe(performance_df, "performance_df", os.path.join(output_dir, Performance.OUTPUT_PERFORMANCE_FILE), append=False)

