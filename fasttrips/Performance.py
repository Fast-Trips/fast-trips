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
    #: Performance column: Trip list ID num
    PERFORMANCE_COLUMN_TRIP_LIST_ID_NUM       = Passenger.TRIP_LIST_COLUMN_TRIP_LIST_ID_NUM
    #: Performance column: Number of label iterations
    PERFORMANCE_COLUMN_LABEL_ITERATIONS       = "label iterations"
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

    #: File with to write performance results
    OUTPUT_PERFORMANCE_FILE                   = 'ft_output_performance.csv'

    def __init__(self):
        """
        Constructor.  Initialize empty dataframe for performance info.
        """
        self.performance_df = pandas.DataFrame(columns=[Performance.PERFORMANCE_COLUMN_ITERATION,
                                                        Performance.PERFORMANCE_COLUMN_TRIP_LIST_ID_NUM,
                                                        Performance.PERFORMANCE_COLUMN_LABEL_ITERATIONS,
                                                        Performance.PERFORMANCE_COLUMN_MAX_STOP_PROCESS_COUNT,
                                                        Performance.PERFORMANCE_COLUMN_TIME_LABELING,
                                                        Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING])

    def add_info(self, iteration, trip_list_id_num, perf_dict):
        """
        Add this row to the performance dataframe.
        Assumes time values are in milliseconds.
        """
        perf_dict[Performance.PERFORMANCE_COLUMN_ITERATION       ] = iteration
        perf_dict[Performance.PERFORMANCE_COLUMN_TRIP_LIST_ID_NUM] = trip_list_id_num

        # convert milliseconds time to timedeltas
        perf_dict[Performance.PERFORMANCE_COLUMN_TIME_LABELING   ] = datetime.timedelta(milliseconds=perf_dict[Performance.PERFORMANCE_COLUMN_TIME_LABELING_MS   ])
        perf_dict[Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING] = datetime.timedelta(milliseconds=perf_dict[Performance.PERFORMANCE_COLUMN_TIME_ENUMERATING_MS])

        self.performance_df = self.performance_df.append(perf_dict, ignore_index=True)

    def write(self, output_dir):
        """
        Writes the results to OUTPUT_PERFORMANCE_FILE to a tab-delimited file.
        """
        Util.write_dataframe(self.performance_df, "performance_df", os.path.join(output_dir, Performance.OUTPUT_PERFORMANCE_FILE))