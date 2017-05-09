__copyright__ = "Copyright 2015 Contributing Entities"
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
import os, sys
from operator import attrgetter
import pandas
import transitfeed

from .Assignment  import Assignment
from .Logger      import FastTripsLogger, setupLogging
from .Passenger   import Passenger
from .Performance import Performance
from .Route       import Route
from .Stop        import Stop
from .TAZ         import TAZ
from .Transfer    import Transfer
from .Trip        import Trip
from .Util        import Util

class FastTrips:
    """
    This is the model itself.  Should be simple and run pieces and store the big data structures.
    """

    #: Info log filename.  Writes brief information about program progression here.
    INFO_LOG  = "ft_info%s.log"

    #: Debug log filename.  Detailed output goes here, including trace information.
    DEBUG_LOG = "ft_debug%s.log"

    def __init__(self, input_network_dir, input_demand_dir, input_weights, run_config, 
        output_dir, input_functions=None, logname_append="", appendLog=False):
        """
        Constructor.

        Reads configuration and input files from *input_network_dir*.
        Writes output files to *output_dir*, including log files.

        :param input_network_dir: Location of network csv files to read
        :type input_network_dir:  string
        :param input_demand_dir:  Location of demand csv files to read
        :type input_demand_dir:   string
        :param input_weights:     Location of pathweight file
        :type input_weights:      string
        :param run_config:        Location of run configuraiton file.
        :type run_config:         string
        :param output_dir:        Location to write output and log files.
        :type output_dir:         string
        :param logname_append:    Modifier for info and debug log filenames.  So workers can write their own logs.
        :type logname_append:     string
        :param appendLog:         Append to info and debug logs?  When FastTrips assignment iterations (to
                                  handle capacity bumps), we'd like to append rather than overwrite.
        :type appendLog:          bool
        """

        #: :py:class:`collections.OrdederedDict` of :py:class:`fasttrips.Passenger` instances indexed by passenger's path ID
        self.passengers      = None

        #: :py:class:`dict` with :py:attr:`fasttrips.Stop.stop_id` key and :py:class:`fasttrips.Stop` value
        self.stops           = None

        #: :py:class:`dict` with :py:attr:`fasttrips.Route.route_id` key and :py:class:`fasttrips.Route` value
        self.routes          = None

        #: :py:class:`dict` with :py:attr:`fasttrips.TAZ.taz_id` key and :py:class:`fasttrips.TAZ` value
        self.tazs            = None

        #: :py:class:`dict` with :py:attr:`fasttrips.Trip.trip_id` key and :py:class:`fasttrips.Trip` value
        self.trips           = None

        #: string representing directory with input network data
        Assignment.INPUT_NETWORK_DIR  = input_network_dir

        #: string representing directory with input demand data
        Assignment.INPUT_DEMAND_DIR   = input_demand_dir

         #: string representing directory with input demand data
        Assignment.INPUT_WEIGHTS      = input_weights

         #: string representing directory with input demand data
        Assignment.CONFIGURATION_FILE = run_config

        #: string representing directory with input demand data
        Assignment.CONFIGURATION_FUNCTIONS_FILE  = input_functions

        #: string representing directory in which to write our output
        Assignment.OUTPUT_DIR         = output_dir

        #: transitfeed schedule instance.  See https://github.com/google/transitfeed
        self.gtfs_schedule      = None

        # setup logging
        setupLogging(os.path.join(Assignment.OUTPUT_DIR, FastTrips.INFO_LOG % logname_append),
                     os.path.join(Assignment.OUTPUT_DIR, FastTrips.DEBUG_LOG % logname_append),
                     logToConsole=True, append=appendLog)

    def read_configuration(self):
        """
        Read the fast-trips assignment and path-finding configuration
        """
        if Assignment.CONFIGURATION_FUNCTIONS_FILE:
            Assignment.read_functions(func_file       = Assignment.CONFIGURATION_FUNCTIONS_FILE)
        Assignment.read_configuration(config_fullpath = Assignment.CONFIGURATION_FILE)
        Assignment.read_weights(weights_file          = Assignment.INPUT_WEIGHTS)

    def read_input_files(self):
        """
        Reads in the input network and demand files and initializes the relevant data structures.
        """
        # Read the gtfs files first
        FastTripsLogger.info("Reading GTFS schedule")
        loader             = transitfeed.Loader(Assignment.INPUT_NETWORK_DIR, memory_db=True)
        self.gtfs_schedule = loader.Load()

        if False:
            # Validate the GTFS
            FastTripsLogger.info("Validating GTFS schedule")
            self.gtfs_schedule.Validate()
            FastTripsLogger.info("Done validating GTFS schedule")

        # Required: Trips, Routes, Stops, Stop Times, Agency, Calendar
        # Optional: Transfers, Shapes, Calendar Dates...

        # Read Stops (gtfs-required)
        self.stops = Stop(Assignment.INPUT_NETWORK_DIR, Assignment.OUTPUT_DIR,
                          self.gtfs_schedule)

        # Read routes, agencies, fares
        self.routes = Route(Assignment.INPUT_NETWORK_DIR, Assignment.OUTPUT_DIR,
                            self.gtfs_schedule, Util.SIMULATION_DAY, self.stops)

        # Read Transfers
        self.transfers = Transfer(Assignment.INPUT_NETWORK_DIR, Assignment.OUTPUT_DIR,
                                  self.gtfs_schedule)

        # Read trips, vehicles, calendar and stoptimes
        self.trips = Trip(Assignment.INPUT_NETWORK_DIR, Assignment.OUTPUT_DIR,
                          self.gtfs_schedule, Util.SIMULATION_DAY,
                          self.stops, self.routes, Assignment.PREPEND_ROUTE_ID_TO_TRIP_ID)

        # read the TAZs into a TAZ instance
        self.tazs = TAZ(Assignment.INPUT_NETWORK_DIR, Assignment.OUTPUT_DIR, Util.SIMULATION_DAY,
                        self.stops, self.transfers, self.routes)

        # Read the demand int passenger_id -> passenger instance
        self.passengers = Passenger(Assignment.INPUT_DEMAND_DIR, Assignment.OUTPUT_DIR, Util.SIMULATION_DAY, self.stops, self.routes, Assignment.CAPACITY_CONSTRAINT)

    def run_assignment(self, output_dir):

        # Initialize performance results
        self.performance = Performance()

        # Do it!  Try it!
        try:
            # do this last before assigning paths so vlaues reflect pathfinding
            Assignment.write_configuration(Assignment.OUTPUT_DIR)

            r = Assignment.assign_paths(output_dir, self)
            FastTripsLogger.info("Successfully completed!")
            return r

        except:
            print("Unexpected error:", sys.exc_info()[0])
            FastTripsLogger.fatal("Unexpected error: %s" % str(sys.exc_info()[0]))
            raise

