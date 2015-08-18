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
import collections,datetime,os,sys
import numpy,pandas

from .Logger import FastTripsLogger

class Trip:
    """
    Trip class.

    One instance represents all of the transit vehicle trips.

    Stores Trip information in :py:attr:`Trip.trips_df`, an instance of :py:class:`pandas.DataFrame`
    and stop time information in :py:attr:`Trip.stop_times_df`, another instance of
    :py:class:`pandas.DataFrame`.
    """

    #: File with trip data.
    #: This is a tab-delimited file with required columns specified by
    #: :py:attr:`Trip.TRIPS_COLUMN_ID`, :py:attr:`Trip.TRIPS_COLUMN_ROUTE_ID`,
    #: :py:attr:`Trip.TRIPS_COLUMN_SERVICE_TYPE`, :py:attr:`Trip.TRIPS_COLUMN_CAPACITY`,
    #: :py:attr:`Trip.TRIPS_COLUMN_SHAPE_ID`, :py:attr:`Trip.TRIPS_COLUMN_DIRECTION_ID`
    INPUT_TRIPS_FILE                    = "ft_input_trips.dat"

    #: Trips column name: Unique identifier.  This will be the index of the trips table.
    TRIPS_COLUMN_ID                     = 'tripId'
    #: Trips column name: Route unique identifier.
    TRIPS_COLUMN_ROUTE_ID               = 'routeId'
    #: Trips column name: Service type:
    #: * 0 - Tram, streetcar, light rail
    #: * 1 - Subway, metro
    #: * 2 - Rail
    #: * 3 - Bus
    #: * 4 - Ferry
    #: * 5 - Cable car
    #: * 6 - Gondola, suspended cable car
    TRIPS_COLUMN_SERVICE_TYPE           = 'type'
    #: Trips column name: Capacity for the vehicle for the trip
    TRIPS_COLUMN_CAPACITY               = 'capacity'
    #: Trips column name: Shape ID
    TRIPS_COLUMN_SHAPE_ID               = 'shapeId'
    #: Trips column name: Direction ID
    TRIPS_COLUMN_DIRECTION_ID           = 'directionId'

    #: File with stop times
    #: This is a tab-delimited file with required columns specified by
    #: :py:attr:`Trip.STOPTIMES_COLUMN_TRIP_ID`, :py:attr:`Trip.STOPTIMES_COLUMN_STOP_ID`,
    #: :py:attr:`Trip.STOPTIMES_COLUMN_ARRIVAL_TIME`, :py:attr:`Trip.STOPTIMES_COLUMN_DEPARTURE_TIME`,
    #: :py:attr:`Trip.STOPTIMES_COLUMN_SEQUENCE`
    INPUT_STOPTIMES_FILE                = "ft_input_stopTimes.dat"

    #: Stop times column name: Trip unique identifier
    STOPTIMES_COLUMN_TRIP_ID            = 'tripId'
    #: Stop times column name: Stop unique identifier
    STOPTIMES_COLUMN_STOP_ID            = 'stopId'
    #: Stop times column name: Sequence number of stop within a trip.
    #: Starts at 1 and is sequential
    STOPTIMES_COLUMN_SEQUENCE           = 'sequence'
    #: Stop times column name: Arrival time string.  e.g. '07:23:05' or '14:08:30'.
    STOPTIMES_COLUMN_ARRIVAL_TIME_STR   = 'arrivalTime_str'
    #: Stop times column name: Arrival time.  This is a float, minutes after midnight.
    STOPTIMES_COLUMN_ARRIVAL_TIME_MIN   = 'arrivalTime_min'
    #: Stop times column name: Arrival time.  This is a DateTime.
    STOPTIMES_COLUMN_ARRIVAL_TIME       = 'arrivalTime'
    #: Stop times column name: Departure time string. e.g. '07:23:05' or '14:08:30'.
    STOPTIMES_COLUMN_DEPARTURE_TIME_STR = 'departureTime_str'
    #: Stop times column name: Departure time. This is a float, minutes after midnight.
    STOPTIMES_COLUMN_DEPARTURE_TIME_MIN = 'departureTime_min'
    #: Stop times column name: Departure time. This is a DateTime.
    STOPTIMES_COLUMN_DEPARTURE_TIME     = 'departureTime'

    #: Default headway if no previous matching route/trip
    DEFAULT_HEADWAY             = 60

    STOPS_IDX_STOP_ID           = 0  #: For accessing parts of :py:attr:`Trip.stops`
    STOPS_IDX_ARRIVAL_TIME      = 1  #: For accessing parts of :py:attr:`Trip.stops`
    STOPS_IDX_DEPARTURE_TIME    = 2  #: For accessing parts of :py:attr:`Trip.stops`

    def __init__(self, input_dir, route_id_to_route, stop_id_to_stop, today):
        """
        Constructor. Read the trips data from the input files in *input_dir*.
        """
        #: Trips table
        self.trips_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_TRIPS_FILE), sep="\t")
        trips_cols = list(self.trips_df.columns.values)
        # verify required columns are present
        assert(Trip.TRIPS_COLUMN_ID             in trips_cols)
        assert(Trip.TRIPS_COLUMN_ROUTE_ID       in trips_cols)
        assert(Trip.TRIPS_COLUMN_SERVICE_TYPE   in trips_cols)
        assert(Trip.TRIPS_COLUMN_CAPACITY       in trips_cols)
        assert(Trip.TRIPS_COLUMN_SHAPE_ID       in trips_cols)
        assert(Trip.TRIPS_COLUMN_DIRECTION_ID   in trips_cols)
        self.trips_df.set_index(Trip.TRIPS_COLUMN_ID, inplace=True, verify_integrity=True)

        # TODO check this
        # assert self.service_type in [0,1,2,3,4,5,6]

        FastTripsLogger.debug("=========== TRIPS ===========\n" + str(self.trips_df.head()))
        FastTripsLogger.debug("\n"+str(self.trips_df.index.dtype)+"\n"+str(self.trips_df.dtypes))
        FastTripsLogger.info("Read %7d trips" % len(self.trips_df))

        # Tell the route about me!
        # TODO: replace with vector version
        # route_id_to_route[self.route_id].add_trip(self)

        #: Stop times table
        self.stop_times_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_STOPTIMES_FILE), sep="\t")
        # verify required columns are present
        stop_times_cols = list(self.stop_times_df.columns.values)
        assert(Trip.STOPTIMES_COLUMN_TRIP_ID        in stop_times_cols)
        assert(Trip.STOPTIMES_COLUMN_STOP_ID        in stop_times_cols)
        assert(Trip.STOPTIMES_COLUMN_SEQUENCE       in stop_times_cols)
        assert(Trip.STOPTIMES_COLUMN_ARRIVAL_TIME   in stop_times_cols)
        assert(Trip.STOPTIMES_COLUMN_DEPARTURE_TIME in stop_times_cols)

        FastTripsLogger.debug("=========== STOP TIMES ===========\n" + str(self.stop_times_df.head()))
        FastTripsLogger.debug("\n"+str(self.stop_times_df.index.dtype)+"\n"+str(self.stop_times_df.dtypes))

        # string version
        self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_STR] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].map(lambda x: '%2d:%02d:%02d' % \
                ( int(x/10000) % 24, int((x % 10000)/100), int(x % 100)))

        self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_STR] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].map(lambda x: '%2d:%02d:%02d' % \
                ( int(x/10000) % 24, int((x % 10000)/100), int(x % 100)))

        # float version
        self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].map(lambda x: \
                60*(int(x/10000) % 24) + int((x % 10000)/100) + int(x % 100)/60.0 )
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].map(lambda x: \
                60*(int(x/10000) % 24) + int((x % 10000)/100) + int(x % 100)/60.0 )

        # datetime version
        self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].map(lambda x: \
                datetime.datetime.combine(today,
                                          datetime.time(hour=int(x/10000) % 24, minute=int((x % 10000)/100), second=int(x % 100))) )
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].map(lambda x: \
                datetime.datetime.combine(today,
                                          datetime.time(hour=int(x/10000) % 24, minute=int((x % 10000)/100), second=int(x % 100))) )

        # TODO: verify sequence information?

        self.stop_times_df.set_index([Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_SEQUENCE], inplace=True, verify_integrity=True)
        FastTripsLogger.debug("Final\n" + str(self.stop_times_df.head()) + "\n" +str(self.stop_times_df.dtypes) )

        #: TODO: this is slow... When Stops gets panda-ized, it won't be an issue
        # tell the stop to update accordingly
        for row_index, row in self.stop_times_df.iterrows():
            stop_id_to_stop[row[Trip.STOPTIMES_COLUMN_STOP_ID]].add_to_trip \
                (row_index[0],                                                  # trip id
                 self.trips_df.loc[row_index[0]][Trip.TRIPS_COLUMN_ROUTE_ID],   # route id
                 row_index[1],                                                  # sequence
                 row[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].time(),                # arrival time
                 row[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].time())              # departure time
        FastTripsLogger.debug("Stops updated")

        # TODO
        # self.headways       = []

        # TODO
        #: Simulation results: list of number of boards per stop
        # self.simulated_boards   = None

        # TODO
        #: Simulation results: list of number of alights per stop
        # self.simulated_alights  = None

        # TODO
        #: Simulation results: list of dwell times per stop
        # self.simulated_dwells   = None

    def get_stop_times(self, trip_id):
        """
        Returns :py:class:`pandas.DataFrame` with stop times for the given trip id.
        """
        return self.stop_times_df.loc[trip_id]

    def number_of_stops(self, trip_id):
        """
        Return the number of stops in this trip.
        """
        return(len(self.stop_times_df.loc[trip_id]))

    def get_scheduled_departure(self, trip_id, stop_id):
        """
        Return the scheduled departure time for the given stop as a datetime.datetime

        TODO: problematic if the stop id occurs more than once in the trip.
        """
        for seq, row in self.stop_times_df.loc[trip_id].iterrows():
            if row[Trip.STOPTIMES_COLUMN_STOP_ID] == stop_id:
                return row[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]
        raise Exception("get_scheduled_departure: stop %s not find for trip %s" % (str(stop_id), str(trip_id)))

    @staticmethod
    def calculate_dwell_times(trips_df):
        """
        Creates dwell_time in the given :py:class:`pandas.DataFrame` instance.
        """
        trips_df['boardsx4']        = trips_df['boards']*4
        trips_df['alightsx2']       = trips_df['alights']*2
        trips_df['dwell_time']      = trips_df[['boardsx4','alightsx2']].max(axis=1) + 4
        # no boards nor alights -> 0
        trips_df.loc[(trips_df.boards==0)&(trips_df.alights==0), 'dwell_time'] = 0
        # tram, streetcar, light rail -> 30 --- this seems arbitrary
        trips_df.loc[trips_df.service_type==0, 'dwell_time']                   = 30

        # drop our intermediate columns
        trips_df.drop(['boardsx4','alightsx2'], axis=1, inplace=True)

        # print "Dwell time > 0:"
        # print trips_df.loc[trips_df.dwell_time>0]

        # these are integers -- make them as such for now
        trips_df[['dwell_time']] = trips_df[['dwell_time']].astype(int)

    @staticmethod
    def calculate_headways(trips_df):
        """
        Calculates headways and sets them into the given
        trips_df :py:class:`pandas.DataFrame`.

        Returns :py:class:`pandas.DataFrame` with `headway` column added.
        """
        stop_group = trips_df[['stop_id','routeId','direction','depart_time','trip_id','stop_seq']].groupby(['stop_id','routeId','direction'])

        stop_group_df = stop_group.apply(lambda x: x.sort('depart_time'))
        # set headway, in minutes
        stop_group_shift_df = stop_group_df.shift()
        stop_group_df['headway'] = (stop_group_df['depart_time'] - stop_group_shift_df['depart_time'])/numpy.timedelta64(1,'m')
        # zero out the first in each group
        stop_group_df.loc[(stop_group_df.stop_id  !=stop_group_shift_df.stop_id  )|
                          (stop_group_df.routeId  !=stop_group_shift_df.routeId  )|
                          (stop_group_df.direction!=stop_group_shift_df.direction), 'headway'] = Trip.DEFAULT_HEADWAY
        # print stop_group_df

        trips_df_len = len(trips_df)
        trips_df = pandas.merge(left=trips_df, right=stop_group_df[['trip_id','stop_id','stop_seq','headway']],
                                on=['trip_id','stop_id','stop_seq'])
        assert(len(trips_df)==trips_df_len)
        return trips_df

    def set_simulation_results(self, boards, alights, dwells):
        """
        Save these simulation results.
        """
        self.simulated_boards   = boards
        self.simulated_alights  = alights
        self.simulated_dwells   = dwells

    @staticmethod
    def write_load_header_to_file(load_file):
        load_file.write("routeId\tshapeId\ttripId\tdirection\tstopId\ttraveledDist\tdepartureTime\t" +
                        "headway\tdwellTime\tboardings\talightings\tload\n")


