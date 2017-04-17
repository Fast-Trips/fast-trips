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
import datetime, os
import pandas

from .Logger    import FastTripsLogger
from .Passenger import Passenger
from .Util      import Util

class Performance:
    """
    Performance class.  Keeps track of performance information
    (time spent, number of labeling iterations, etc) related to pathfinding
    in Fast-Trips.
    """
    #: Performance column: Iteration
    PERFORMANCE_COLUMN_ITERATION              = "iteration"
    #: Performance column: Pathfinding Iteration
    PERFORMANCE_COLUMN_PATHFINDING_ITERATION  = "pathfinding_iteration"
    #: Performance column: Person ID
    PERFORMANCE_COLUMN_PERSON_ID              = Passenger.TRIP_LIST_COLUMN_PERSON_ID
    #: Performance column: Person Trip ID
    PERFORMANCE_COLUMN_PERSON_TRIP_ID         = Passenger.TRIP_LIST_COLUMN_PERSON_TRIP_ID
    #: Performance column: Process number
    PERFORMANCE_COLUMN_PROCESS_NUM            = "process number"
    #: Performance column: Pathfinding status
    PERFORMANCE_COLUMN_PATHFINDING_STATUS     = "pathfinding_status"
    #: Performance column: Number of label iterations
    PERFORMANCE_COLUMN_LABEL_ITERATIONS       = "label iterations"
    #: Performance column: Number of labeled stops
    PERFORMANCE_COLUMN_NUM_LABELED_STOPS      = "num labeled stops"
    #: Performance column: Maximum number of times a stop was processed
    PERFORMANCE_COLUMN_MAX_STOP_PROCESS_COUNT = "max stop process count"
    #: Performance column: Time spent labeling (timedelta)
    PERFORMANCE_COLUMN_TIME_LABELING          = "time labeling"
    #: Performance column: Time spent labeling (milliseconds)
    PERFORMANCE_COLUMN_TIME_LABELING_MS       = "time labeling milliseconds"
    #: Performance column: Time spent enumerating (timedelta)
    PERFORMANCE_COLUMN_TIME_ENUMERATING       = "time enumerating"
    #: Performance column: Time spent enumerating (milliseconds)
    PERFORMANCE_COLUMN_TIME_ENUMERATING_MS    = "time enumerating milliseconds"
    #: Performance column: Traced, since this affects performance
    PERFORMANCE_COLUMN_TRACED                 = "traced"
    #: Performance column: Working set in memory, in bytes
    PERFORMANCE_COLUMN_WORKING_SET_BYTES      = "working set bytes"
    #: Performance column: Private usage in memroy, in bytes
    PERFORMANCE_COLUMN_PRIVATE_USAGE_BYTES    = "private usage bytes"

    #: File with to write performance results
    OUTPUT_PERFORMANCE_FILE                   = 'ft_output_performance.csv'

    def __init__(self):
        """
        Constructor.  Initialize empty dataframe for performance info.
        """
        self.performance_dict = {
            Performance.PERFORMANCE_COLUMN_ITERATION                :[],
            Performance.PERFORMANCE_COLUMN_PATHFINDING_ITERATION    :[],
            Performance.PERFORMANCE_COLUMN_PERSON_ID                :[],
            Performance.PERFORMANCE_COLUMN_PERSON_TRIP_ID           :[],
            Performance.PERFORMANCE_COLUMN_PROCESS_NUM              :[],
            Performance.PERFORMANCE_COLUMN_PATHFINDING_STATUS       :[],
            Performance.PERFORMANCE_COLUMN_NUM_LABELED_STOPS        :[],
            Performance.PERFORMANCE_COLUMN_TRACED                   :[],
            Performance.PERFORMANCE_COLUMN_LABEL_ITERATIONS         :[],
            Performance.PERFORMANCE_COLUMN_MAX_STOP_PROCESS_COUNT   :[],
            Performance.PERFORMANCE_COLUMN_TIME_LABELING            :[],
            Performance.PERFORMANCE_COLUMN_TIME_LABELING_MS         :[],
            Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING         :[],
            Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING_MS      :[],
            Performance.PERFORMANCE_COLUMN_WORKING_SET_BYTES        :[],
            Performance.PERFORMANCE_COLUMN_PRIVATE_USAGE_BYTES      :[]
        }


    def add_info(self, iteration, pathfinding_iteration, person_id, person_trip_id,  perf_dict):
        """
        Add this row to the performance dict of arrays.
        Assumes time values are in milliseconds.
        """
        self.performance_dict[Performance.PERFORMANCE_COLUMN_ITERATION].append(iteration)
        self.performance_dict[Performance.PERFORMANCE_COLUMN_PATHFINDING_ITERATION].append(pathfinding_iteration)
        self.performance_dict[Performance.PERFORMANCE_COLUMN_PERSON_ID].append(person_id)
        self.performance_dict[Performance.PERFORMANCE_COLUMN_PERSON_TRIP_ID].append(person_trip_id)

        for key in [Performance.PERFORMANCE_COLUMN_PROCESS_NUM,
                    Performance.PERFORMANCE_COLUMN_PATHFINDING_STATUS,
                    Performance.PERFORMANCE_COLUMN_LABEL_ITERATIONS,
                    Performance.PERFORMANCE_COLUMN_NUM_LABELED_STOPS,
                    Performance.PERFORMANCE_COLUMN_TRACED,
                    Performance.PERFORMANCE_COLUMN_MAX_STOP_PROCESS_COUNT,
                    Performance.PERFORMANCE_COLUMN_TIME_LABELING_MS,
                    Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING_MS,
                    Performance.PERFORMANCE_COLUMN_WORKING_SET_BYTES,
                    Performance.PERFORMANCE_COLUMN_PRIVATE_USAGE_BYTES]:
            self.performance_dict[key].append(perf_dict[key])

        # convert milliseconds time to timedeltas
        self.performance_dict[Performance.PERFORMANCE_COLUMN_TIME_LABELING   ].append(datetime.timedelta(milliseconds=perf_dict[Performance.PERFORMANCE_COLUMN_TIME_LABELING_MS   ]))
        self.performance_dict[Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING].append(datetime.timedelta(milliseconds=perf_dict[Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING_MS]))

    def write(self, output_dir, append):
        """
        Writes the results to OUTPUT_PERFORMANCE_FILE to a tab-delimited file.
        """
        performance_df = pandas.DataFrame.from_dict(self.performance_dict)

        Util.write_dataframe(performance_df, "performance_df", os.path.join(output_dir, Performance.OUTPUT_PERFORMANCE_FILE), append=append)

        # reset dict to blank
        for key in self.performance_dict.keys():
            self.performance_dict[key] = []