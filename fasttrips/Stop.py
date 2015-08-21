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
import pandas

from .Logger import FastTripsLogger

class Stop:
    """
    Stop class.
    
    One instance represents all of the Stops as well as their transfer links.
    
    Stores stop information in :py:attr:`Stop.stops_df`, an instance of :py:class:`pandas.DataFrame`
    and transfer link information in :py:attr:`Stop.transfers_df`, another instance of
    :py:class:`pandas.DataFrame`.
    """

    #: File with stops.
    #: This is a tab-delimited file with required columns specified by
    #: :py:attr:`Stop.STOPS_COLUMN_ID`, :py:attr:`Stop.STOPS_COLUMN_NAME`
    #: :py:attr:`Stop.STOPS_COLUMN_DESCRIPTION`,
    #: :py:attr:`Stop.STOPS_COLUMN_LATITUDE`, :py:attr:`Stop.STOPS_COLUMN_LONGITUDE`
    #: :py:attr:`Stop.STOPS_COLUMN_CAPACITY`
    INPUT_STOPS_FILE            = "ft_input_stops.dat"
    #: Stops column name: Unique identifier
    STOPS_COLUMN_ID             = 'stopId'
    #: Stops column name: Stop name (string)
    STOPS_COLUMN_NAME           = 'stopName'
    #: Stops column name: Stop description (string)
    STOPS_COLUMN_DESCRIPTION    = 'stopDescription'
    #: Stops column name: Latitude
    STOPS_COLUMN_LATITUDE       = 'Latitude'
    #: Stops column name: Longitude
    STOPS_COLUMN_LONGITUDE      = 'Longitude'
    #: Stops column name: Capacity
    STOPS_COLUMN_CAPACITY       = 'capacity'
    
    #: File with transfers.
    #: TODO: document format
    INPUT_TRANSFERS_FILE        = "ft_input_transfers.dat"
    #: Transfers column name: Origin stop identifier
    TRANSFERS_COLUMN_FROM_STOP  = 'fromStop'
    #: Transfers column name: Destination stop identifier
    TRANSFERS_COLUMN_TO_STOP    = 'toStop'
    #: Transfers column name: Link walk distance
    TRANSFERS_COLUMN_DISTANCE   = 'dist'
    #: Transfers column name: Link walk time.  This is a TimeDelta.
    TRANSFERS_COLUMN_TIME       = 'time'
    #: Transfers column name: Link walk time in minutes.  This is a float.
    TRANSFERS_COLUMN_TIME_MIN   = 'time_min'

    TRANSFERS_IDX_DISTANCE      = 0  #: For accessing parts of :py:attr:`Stop.transfers`
    TRANSFERS_IDX_TIME          = 1  #: For accessing parts of :py:attr:`Stop.transfers`

    TRIPS_IDX_TRIP_ID           = 0  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_SEQUENCE          = 1  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_ARRIVAL_TIME      = 2  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_DEPARTURE_TIME    = 3  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_ROUTE_ID          = 4  #: For accessing parts of :py:attr:`Stop.trips`

    def __init__(self, input_dir):
        """
        Constructor.  Reads the Stops data from the input files in *input_dir*.
        """
        pandas.set_option('display.width', 1000)
        self.stops_df = pandas.read_csv(os.path.join(input_dir, Stop.INPUT_STOPS_FILE), sep="\t")
        # verify required columns are present
        stops_cols = list(self.stops_df.columns.values)
        assert(Stop.STOPS_COLUMN_ID             in stops_cols)
        assert(Stop.STOPS_COLUMN_NAME           in stops_cols)
        assert(Stop.STOPS_COLUMN_DESCRIPTION    in stops_cols)
        assert(Stop.STOPS_COLUMN_LATITUDE       in stops_cols)
        assert(Stop.STOPS_COLUMN_LONGITUDE      in stops_cols)
        assert(Stop.STOPS_COLUMN_CAPACITY       in stops_cols)
        self.stops_df.set_index(Stop.STOPS_COLUMN_ID, inplace=True, verify_integrity=True)
        
        FastTripsLogger.debug("=========== STOPS ===========\n" + str(self.stops_df.head()))
        FastTripsLogger.debug("\n"+str(self.stops_df.index.dtype)+"\n"+str(self.stops_df.dtypes))
        FastTripsLogger.info("Read %7d stops" % len(self.stops_df))

        self.transfers_df = pandas.read_csv(os.path.join(input_dir, Stop.INPUT_TRANSFERS_FILE), sep="\t")
        # verify required columns are present
        transfer_cols = list(self.transfers_df.columns.values)
        assert(Stop.TRANSFERS_COLUMN_FROM_STOP  in transfer_cols)
        assert(Stop.TRANSFERS_COLUMN_TO_STOP    in transfer_cols)
        assert(Stop.TRANSFERS_COLUMN_DISTANCE   in transfer_cols)
        assert(Stop.TRANSFERS_COLUMN_TIME       in transfer_cols)


        FastTripsLogger.debug("=========== TRANSFERS ===========\n" + str(self.transfers_df.head()))
        FastTripsLogger.debug("\n"+str(self.transfers_df.dtypes))
        
        # ignore time column and calculate it from distance
        # TODO: this is to be consistent with original implementation. Remove?
        self.transfers_df[Stop.TRANSFERS_COLUMN_TIME_MIN] = self.transfers_df[Stop.TRANSFERS_COLUMN_DISTANCE]*60.0/3.0;
        # convert time column from float to timedelta
        self.transfers_df[Stop.TRANSFERS_COLUMN_TIME] = \
            self.transfers_df[Stop.TRANSFERS_COLUMN_TIME_MIN].map(lambda x: datettime.timedelta(minutes=x))
                
        transfer_records = transfers_df.to_dict(orient='records')
        for transfer_record in transfer_records:
            stop_id_to_stop[transfer_record['fromStop']].add_transfer(transfer_record)

        FastTripsLogger.debug("Final\n"+str(self.transfers_df.dtypes))
        FastTripsLogger.info("Read %7d transfers" % len(self.transfers_df))

        #: These are TAZs that are accessible from this stop.
        #: This is a :py:class:`dict` mapping a *taz_id* to
        #: (walk_dist, walk_time) which are in miles and minutes respectively.
        self.tazs               = {}

        #: These are the :py:class:`fasttrips.Route` instances that this stop is a part of
        self.routes             = set()

        #: These are the trips this stop is a part of
        #: This is a list of (trip_id, sequence, arrival time, departure time)
        #: Use :py:attr:`Stop.TRIPS_IDX_TRIP_ID`, :py:attr:`Stop.TRIPS_IDX_SEQUENCE`,
        #: :py:attr:`Stop.TRIPS_IDX_ARRIVAL_TIME`, :py:attr:`Stop.TRIPS_IDX_DEPARTURE_TIME`
        #: and :py:attr:`Stop.TRIPS_IDX_ROUTE_ID` for access.
        self.trips              = []

    def add_access_link(self, access_link_record):
        """
        Add an access link between this stop and a :py:class:`TAZ` by referencing the *taz_id*.
        """
        self.tazs[access_link_record['TAZ']] = (access_link_record['dist'],
                                                access_link_record['time'])

    def add_to_trip(self, trip_id, route_id, sequence, arrival_time, departure_time):
        """
        Add myself to the given trip.

        :param trip: The trip of which this stop is a part.
        :type trip: a :py:class:`Trip` instance
        :param sequence: The sequence number of this stop within the trip
        :param arrival_time: The arrival time at this stop for this trip.
        :type arrival_time: a :py:class:`datetime.time` instance
        :param departure_time: The departure time at this stop for this trip.
        :type departure_time: a :py:class:`datetime.time` instance

        """
        self.trips.append( (trip_id, sequence, arrival_time, departure_time, route_id) )
        # and route
        self.routes.add(route_id)

    def get_trips_arriving_within_time(self, assignment_date, latest_arrival, time_window):
        """
        Return list of [(trip_id, sequence, arrival_time)] where the arrival time is before *latest_arrival* but within *time_window*.

        :param assignment_date: Date to use for assignment (:py:class:`datetime.timedelta` requires :py:class:`datetime.datettime` instances,
                                and can't just use :py:class:`datetime.time` instances.)
        :type assignment_date: a :py:class:`datetime.date` instance
        :param latest_arrival: The latest time the transit vehicle can arrive.
        :type latest_arrival: a :py:class:`datetime.time` instance
        :param time_window: The time window extending before *latest_arrival* within which an arrival is valid.
        :type time_window: a :py:class:`datetime.timedelta` instance

        """
        to_return = []
        for trip_record in self.trips:
            # make this a datetime
            asgn_arrival = datetime.datetime.combine(assignment_date, trip_record[Stop.TRIPS_IDX_ARRIVAL_TIME])
            if (asgn_arrival < latest_arrival) and (asgn_arrival > latest_arrival-time_window):
               to_return.append( (trip_record[Stop.TRIPS_IDX_TRIP_ID],
                                  trip_record[Stop.TRIPS_IDX_SEQUENCE],
                                  trip_record[Stop.TRIPS_IDX_ARRIVAL_TIME]) )
        return to_return

    def get_trips_departing_within_time(self, assignment_date, earliest_departure, time_window):
        """
        Return list of [(trip_id, sequence, departure_time)] where the departure time is after *earliest_departure* but within *time_window*.

        :param assignment_date: Date to use for assignment (:py:class:`datetime.timedelta` requires :py:class:`datetime.datettime` instances,
                                and can't just use :py:class:`datetime.time` instances.)
        :type assignment_date: a :py:class:`datetime.date` instance
        :param earliest_departure: The earliest time the transit vehicle can depart.
        :type earliest_departure: a :py:class:`datetime.time` instance
        :param time_window: The time window extending after *earliest_departure* within which a departure is valid.
        :type time_window: a :py:class:`datetime.timedelta` instance

        """
        to_return = []
        for trip_record in self.trips:
            # make this a datetime
            asgn_departure = datetime.datetime.combine(assignment_date, trip_record[Stop.TRIPS_IDX_DEPARTURE_TIME])
            if (asgn_departure > earliest_departure) and (asgn_departure < earliest_departure+time_window):
               to_return.append( (trip_record[Stop.TRIPS_IDX_TRIP_ID],
                                  trip_record[Stop.TRIPS_IDX_SEQUENCE],
                                  trip_record[Stop.TRIPS_IDX_DEPARTURE_TIME]) )
        return to_return

    def get_previous_trip_departure(self, FT, route_id, trip_direction, before_departure_time):
        """
        Goes through the routes/trips that use this stop, and returns the most recent trip departure before *before_departure_time*.

        Returns a :py:class:`datetime.time` instance or None if none found.
        """
        found                 = False
        prev_departure_time   = None
        if self.stop_id == 68584 and route_id == 102552 and trip_direction == 1:
            FastTripsLogger.debug("Stop %d get previous trip departure for route %d trip direction %d before departure time %s" % \
                                  (self.stop_id, route_id, trip_direction, before_departure_time.strftime("%H:%M:%S")))
        for idx in range(len(self.trips)):
            # match route
            if self.trips[idx][Stop.TRIPS_IDX_ROUTE_ID] != route_id: continue

            # match direction
            trip = FT.trips[self.trips[idx][Stop.TRIPS_IDX_TRIP_ID]]
            if trip.direction_id != trip_direction: continue

            # it's before the required
            trip_deptime = self.trips[idx][Stop.TRIPS_IDX_DEPARTURE_TIME]
            if self.stop_id == 68584 and route_id == 102552 and trip_direction == 1:
                FastTripsLogger.debug("Matching route, direction: %d.  trip_deptime: %s" % (trip.trip_id, str(trip_deptime)))
            if trip_deptime >= before_departure_time: continue

            if not found:
                found                 = True
                prev_departure_time   = trip_deptime
            elif trip_deptime > prev_departure_time: # want the latest one
                prev_departure_time = trip_deptime

        if self.stop_id == 68584 and route_id == 102552 and trip_direction == 1:
            FastTripsLogger.debug("Returning %s" % prev_departure_time.strftime("%H:%M:%S") if prev_departure_time else "--")
        return prev_departure_time


    def is_transfer(self, stop_id):
        """
        Returns true iff this is a transfer stop; e.g. if it's served by multiple routes or has a transfer link.
        """
            # if len(self.routes) > 1:
            # return True
        if len(self.transfers_df.loc[self.transfers_df.TRANSFERS_COLUMN_FROM_STOP==stop_id]) > 0:
            return True
        return False

