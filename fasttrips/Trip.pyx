# cython: profile=True
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
    Trip class.  This is a transit vehicle trip.
    """

    #: File with trips.
    #: TODO document format
    INPUT_TRIPS_FILE            = "ft_input_trips.dat"

    #: File with stop times
    #: TODO document format
    INPUT_STOPTIMES_FILE        = "ft_input_stopTimes.dat"

    #: Default headway if no previous matching route/trip
    DEFAULT_HEADWAY             = 60

    STOPS_IDX_STOP_ID           = 0  #: For accessing parts of :py:attr:`Trip.stops`
    STOPS_IDX_ARRIVAL_TIME      = 1  #: For accessing parts of :py:attr:`Trip.stops`
    STOPS_IDX_DEPARTURE_TIME    = 2  #: For accessing parts of :py:attr:`Trip.stops`

    def __init__(self, trip_record, route_id_to_route):
        """
        Constructor from dictionary mapping attribute to value.
        """
        #: unique trip identifier
        self.trip_id        = trip_record['tripId']

        #: corresponds to :py:attr:`fasttrips.Route.route_id`
        self.route_id       = trip_record['routeId']

        #: Service type:
        #: 0 - Tram, streetcar, light rail
        #: 1 - Subway, metro
        #: 2 - Rail
        #: 3 - Bus
        #: 4 - Ferry
        #: 5 - Cable car
        #: 6 - Gondola, suspended cable car
        self.service_type   = trip_record['type']
        assert self.service_type in [0,1,2,3,4,5,6]

        #: The vehicle capacity for the trip
        self.capacity       = trip_record['capacity']

        #: ID that defines a shape for the trip
        self.shape_id       = trip_record['shapeId']

        #: binary varlue (0 or 1) indicating the direction of the trip
        self.direction_id   = trip_record['directionId']

        # Tell the route about me!
        route_id_to_route[self.route_id].add_trip(self)

        #: List of (stop_id, arrival time, departure time)
        #: Use :py:attr:`Trip.STOPS_IDX_STOP_ID`, :py:attr:`Trip.STOPS_IDX_ARRIVAL_TIME`, and
        #: :py:attr:`Trip.STOPS_IDX_DEPARTURE_TIME` for access.
        #: Times are :py:class:`datetime.time` instances.
        self.stops          = []
        self.headways       = []

        #: Simulation results: list of number of boards per stop
        self.simulated_boards   = None

        #: Simulation results: list of number of alights per stop
        self.simulated_alights  = None

        #: Simulation results: list of dwell times per stop
        self.simulated_dwells   = None

    def number_of_stops(self):
        """
        Return the number of stops in this trip.
        """
        return(len(self.stops))

    def add_stop_time(self, stop_time_record, stop_id_to_stop):
        """
        Add the stop time information to this trip.

        The times are in HHMMSS format.
        For example, 60738 = 6:07:38

        """
        # verify they're in sequence and they start at one
        assert(stop_time_record['sequence']) == len(self.stops)+1

        # the sequence is the index
        self.stops.append( (stop_time_record['stopId'],
                            datetime.time(hour = int(stop_time_record['arrivalTime']/10000) % 24,
                                          minute = int((stop_time_record['arrivalTime'] % 10000)/100),
                                          second = int(stop_time_record['arrivalTime'] % 100)),
                            datetime.time(hour = int(stop_time_record['departureTime']/10000) % 24,
                                          minute = int((stop_time_record['departureTime'] % 10000)/100),
                                          second = int(stop_time_record['departureTime'] % 100))
                        ) )

        # tell the stop to update accordingly
        stop_id_to_stop[stop_time_record['stopId']].add_to_trip(self,
                                                                stop_time_record['sequence'],
                                                                self.stops[-1][Trip.STOPS_IDX_ARRIVAL_TIME],
                                                                self.stops[-1][Trip.STOPS_IDX_DEPARTURE_TIME])
    def get_scheduled_departure(self, stop_id):
        """
        Return the scheduled departure time for the given stop
        """
        for stop in self.stops:
            if stop[Trip.STOPS_IDX_STOP_ID] == stop_id:
                return stop[Trip.STOPS_IDX_DEPARTURE_TIME]
        raise Exception("get_scheduled_departure: stop %s not find for trip %s" % (str(stop_id), str(self.trip_id)))

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


    def calculate_dwell_time(self, number_of_boards, number_of_alights):
        """
        Calculates the dwell time at a stop given the nuumber of boards and alights at the stop.
        Returns a :py:class:`datetime.timedelta` instance.

        TODO: should this be configurable?
        """
        if self.service_type == 0:
            return datetime.timedelta(seconds=30)

        if (number_of_boards>0) or (number_of_alights>0):
            return datetime.timedelta(seconds=(4+max(4*number_of_boards, 2*number_of_alights)))

        return datetime.timedelta(seconds=0)

    @staticmethod
    def calculate_headways(trips_df):
        """
        Calculates headways and sets them into the given
        trips_df :py:class:`pandas.DataFrame`.

        Returns :py:class:`pandas.DataFrame` with `headway` column added.
        """
        stop_group = trips_df[['stop_id','route_id','direction','depart_time','trip_id','stop_seq']].groupby(['stop_id','route_id','direction'])

        stop_group_df = stop_group.apply(lambda x: x.sort('depart_time'))
        # set headway, in minutes
        stop_group_shift_df = stop_group_df.shift()
        stop_group_df['headway'] = (stop_group_df['depart_time'] - stop_group_shift_df['depart_time'])/numpy.timedelta64(1,'m')
        # zero out the first in each group
        stop_group_df.loc[(stop_group_df.stop_id  !=stop_group_shift_df.stop_id  )|
                          (stop_group_df.route_id !=stop_group_shift_df.route_id )|
                          (stop_group_df.direction!=stop_group_shift_df.direction), 'headway'] = Trip.DEFAULT_HEADWAY
        # print stop_group_df

        trips_df_len = len(trips_df)
        trips_df = pandas.merge(left=trips_df, right=stop_group_df[['trip_id','stop_id','stop_seq','headway']],
                                on=['trip_id','stop_id','stop_seq'])
        assert(len(trips_df)==trips_df_len)
        return trips_df

    def calculate_headway(self, FT, today):
        """
        Calculates the headway for each stop in this trip by finding the previous departure time for vehicles
        serving this same stop, route and direction.

        Saves into :py:attr:`Trip.headways` list of :py:class:`datetime.timedelta` instances.

        .. todo:: Some of the stop times might be after midnight; e.g. 24:21:21, making it so a previous trip departure like
                  23:51:51 would be found in the C++ code.  We convert all times to :py:class:`datetime.time` instances so we
                  get different results here when this conversion affects the headway calculation.

        """
        self.headways = []
        for stop_idx in range(len(self.stops)):
            stop_id      = self.stops[stop_idx][Trip.STOPS_IDX_STOP_ID]
            arrival_time = self.stops[stop_idx][Trip.STOPS_IDX_ARRIVAL_TIME]
            prev_dep     = FT.stops[stop_id].get_previous_trip_departure(FT, self.route_id, self.direction_id, arrival_time)

            if prev_dep == None:
                headway  = datetime.timedelta(minutes=Trip.DEFAULT_HEADWAY)
            else:
                headway  = datetime.datetime.combine(today, arrival_time) - \
                           datetime.datetime.combine(today, prev_dep)
            self.headways.append(headway)

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

    def write_load_to_file(self, load_file):
        """
        Write simulation results to the given file.  Fields are:

        * routeId
        * shapeId
        * tripId
        * direction
        * stopId
        * traveled distance -- not implemented, just prints -1
        * departure time, in minutes from the start of day
        * headway
        * dwell time of vehicle at stop in seconds
        * number of boardings
        * number of alightings
        * load

        """
        stop_idx = 0
        on_board = 0
        for stop in self.stops:
            on_board += self.simulated_boards[stop_idx]
            on_board -= self.simulated_alights[stop_idx]
            load_file.write("%s\t%s\t%s\t%s\t%s\t" % \
                            (str(self.route_id),
                             str(self.shape_id),
                             str(self.trip_id),
                             self.direction_id,
                             str(stop[Trip.STOPS_IDX_STOP_ID])))
            load_file.write("-1\t%.3f\t%s\t%.2f\t%d\t%d\t%d\n" % \
                            ((stop[Trip.STOPS_IDX_DEPARTURE_TIME].hour*60.0 +
                              stop[Trip.STOPS_IDX_DEPARTURE_TIME].minute +
                              stop[Trip.STOPS_IDX_DEPARTURE_TIME].second/60.0),
                             self.headways[stop_idx].total_seconds()/60.0,
                             self.simulated_dwells[stop_idx].total_seconds(),
                             self.simulated_boards[stop_idx],
                             self.simulated_alights[stop_idx],
                             on_board))
            stop_idx += 1


    @staticmethod
    def read_trips(input_dir, route_id_to_route):
        """
        Read the trips from the input file in *input_dir*.
        """
        trips_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_TRIPS_FILE), sep="\t")
        FastTripsLogger.debug("=========== TRIPS ===========\n" + str(trips_df.head()))
        FastTripsLogger.debug("\n"+str(trips_df.dtypes))

        trip_id_to_trip = collections.OrderedDict()
        trip_records = trips_df.to_dict(orient='records')
        for trip_record in trip_records:
            trip = Trip(trip_record, route_id_to_route)
            trip_id_to_trip[trip.trip_id] = trip

        FastTripsLogger.info("Read %7d trips" % len(trip_id_to_trip))
        return trip_id_to_trip


    @staticmethod
    def read_stop_times(input_dir, trip_id_to_trip, stop_id_to_stop):
        """
        Read the stop times from the input file in *input_dir*.

        TODO: This loses int types for stop_ids, etc.
        """
        stop_times_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_STOPTIMES_FILE), sep="\t")
        FastTripsLogger.debug("=========== STOP TIMES ===========\n" + str(stop_times_df.head()))
        FastTripsLogger.debug("\n"+str(stop_times_df.dtypes))

        stop_time_records = stop_times_df.to_dict(orient='records')
        for stop_time_record in stop_time_records:
            trip = trip_id_to_trip[stop_time_record['tripId']]
            trip.add_stop_time(stop_time_record, stop_id_to_stop)

        FastTripsLogger.info("Read %7d stop times" % (len(stop_times_df)))


