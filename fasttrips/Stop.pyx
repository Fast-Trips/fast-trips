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
import pandas

from .Logger import FastTripsLogger

epoch = datetime.date(1970,1,1)


class Stop:
    """
    Stop class.  Documentation forthcoming.
    """

    #: File with stops.
    #: TODO document format
    INPUT_STOPS_FILE            = "ft_input_stops.dat"

    #: File with transfers.
    #: TODO: document format
    INPUT_TRANSFERS_FILE        = "ft_input_transfers.dat"

    TRANSFERS_IDX_DISTANCE      = 0  #: For accessing parts of :py:attr:`Stop.transfers`
    TRANSFERS_IDX_TIME          = 1  #: For accessing parts of :py:attr:`Stop.transfers`

    TRIPS_IDX_TRIP_ID           = 0  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_SEQUENCE          = 1  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_ARRIVAL_TIME      = 2  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_DEPARTURE_TIME    = 3  #: For accessing parts of :py:attr:`Stop.trips`
    TRIPS_IDX_ROUTE_ID          = 4  #: For accessing parts of :py:attr:`Stop.trips`

    def __init__(self, stop_record):
        """
        Constructor from dictionary mapping attribute to value.

        {'stopName': 'AURORA AVE N & N 125TH ST',
         'capacity': 100L,
         'stopDescription': '_',
         'Longitude': -122.345047,
         'stopId': 7010L,
         'Latitude': 47.7197151}
        """
        #: unique stop identifier
        self.stop_id            = stop_record['stopId'          ]
        self.name               = stop_record['stopName'        ]
        self.description        = stop_record['stopDescription' ]
        self.latitude           = stop_record['Latitude'        ]
        self.longitude          = stop_record['Longitude'       ]
        self.capacity           = stop_record['capacity'        ]

        #: These are stops that are transferrable from this stop.
        #: This is a :py:class:`dict` mapping a destination *stop_id* to
        #: (transfer_distance, transfer_time), where *transfer_distance* is in miles
        #: and *transfer_time* is a :py:class:`datetime.timedelta` instance.
        #: TODO: This can be a dict, just making it an ordered dict for testing
        self.transfers          = collections.OrderedDict()

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

    def add_transfer(self, transfer_record):
        """
        Add a transfer from this stop to the stop given in the transfer_record dictionary.

        .. todo:: Original code replaces transfer time with calculated time assuming 3 mph walk speed.  Remove this?

        """
        # self.stop_id == transfer_record['fromStop']
        self.transfers[transfer_record['toStop']] = (transfer_record['dist'],
                                                     datetime.timedelta(minutes=transfer_record['dist']*(60.0/3.0)))
                                                     # datetime.timedelta(minutes=transfer_record['time']))

    def add_access_link(self, access_link_record):
        """
        Add an access link between this stop and a :py:class:`TAZ` by referencing the *taz_id*.
        """
        self.tazs[access_link_record['TAZ']] = (access_link_record['dist'],
                                                access_link_record['time'])

    def add_to_trip(self, trip, sequence, arrival_time, departure_time):
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
        self.trips.append( (trip.trip_id, sequence, arrival_time, departure_time, trip.route_id) )
        # and route
        self.routes.add(trip.route_id)

    @staticmethod
    def get_trips_arriving_within_time(trips, assignment_date, latest_arrival, time_window):
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

        cdef:
          list to_return
          int i
          tuple trip_record
          int asgn_arrival
          int ad_y, ad_m, ad_d, ed_h, ed_m, ed_s, tw_d, tw_s
          int ed_tot, tw_tot, ed_plus_tw
          int ad_epoch
          int trip_idx_id, trip_idx_seq, trip_idx_time

        trip_idx_id = Stop.TRIPS_IDX_TRIP_ID
        trip_idx_seq = Stop.TRIPS_IDX_SEQUENCE
        trip_idx_time = Stop.TRIPS_IDX_ARRIVAL_TIME

        to_return = []

        # Seconds since 1970 'epoch'
        ad_epoch = (assignment_date - epoch).total_seconds()

        # Latest allowable arrival, in seconds since epoch
        ed_h = latest_arrival.hour
        ed_m = latest_arrival.minute
        ed_s = latest_arrival.second
        ed_tot = ad_epoch + ed_s + 60*ed_m + 3600*ed_h

        # Total seconds in the acceptable time window
        tw_d = time_window.days
        tw_s = time_window.seconds
        tw_tot = tw_s + tw_d*86400

        # earliest allowable arrival, in seconds since epoch
        ed_minus_tw = ed_tot - tw_tot

        # i-loop is faster in cython
        for i in xrange(len(trips)):
            trip_record = trips[i]

            asgn_d = trip_record[trip_idx_time]
            asgn_arrival = ad_epoch + asgn_d.second + 60*asgn_d.minute + 3600*asgn_d.hour

            if (asgn_arrival < ed_tot) and (asgn_arrival > ed_minus_tw):
               to_return.append( (trip_record[trip_idx_id],
                                  trip_record[trip_idx_seq],
                                  trip_record[trip_idx_time]) )
        return to_return

    @staticmethod
    def get_trips_departing_within_time(trips, assignment_date, earliest_departure, time_window):
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
        cdef:
          list to_return
          int i
          tuple trip_record
          int asgn_departure
          int ad_y, ad_m, ad_d, ed_h, ed_m, ed_s, tw_d, tw_s
          int ed_tot, tw_tot, ed_plus_tw
          int ad_epoch
          int trip_idx_id, trip_idx_seq, trip_idx_time

        trip_idx_id = Stop.TRIPS_IDX_TRIP_ID
        trip_idx_seq = Stop.TRIPS_IDX_SEQUENCE
        trip_idx_time = Stop.TRIPS_IDX_DEPARTURE_TIME

        to_return = []

        # This used to be a really simple datetime.combine(), but that turns out to be really slow
        # inside a loop called a billion times. Switching to this ugly integer math, which cython
        # compiles into very fast C code. Sorry for the ugly.

        # Seconds since 1970 'epoch'
        ad_epoch = (assignment_date - epoch).total_seconds()

        # Earliest allowable departure, in seconds since epoch
        ed_h = earliest_departure.hour
        ed_m = earliest_departure.minute
        ed_s = earliest_departure.second
        ed_tot = ad_epoch + ed_s + 60*ed_m + 3600*ed_h

        # Total seconds in the acceptable time window
        tw_d = time_window.days
        tw_s = time_window.seconds
        tw_tot = tw_s + tw_d*86400

        # latest allowable departure, in seconds since epoch
        ed_plus_tw = ed_tot + tw_tot

        # i-loop is faster in cython
        for i in xrange(len(trips)):
            trip_record = trips[i]

            # old datetime code:
            # asgn_departure = datetime.datetime.combine(assignment_date, trip_record[Stop.TRIPS_IDX_DEPARTURE_TIME])
            # if (asgn_departure > earliest_departure) and (asgn_departure < earliest_departure+time_window):
            asgn_d = trip_record[trip_idx_time]
            asgn_departure = ad_epoch + asgn_d.second + 60*asgn_d.minute + 3600*asgn_d.hour

            if (asgn_departure > ed_tot) and (asgn_departure < ed_plus_tw):
               to_return.append( (trip_record[trip_idx_id],
                                  trip_record[trip_idx_seq],
                                  trip_record[trip_idx_time]) )
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


    def is_transfer(self):
        """
        Returns true iff this is a transfer stop; e.g. if it's served by multiple routes or has a transfer link.
        """
        if len(self.routes) > 1:
            return True
        if len(self.transfers) > 0:
            return True
        return False

    @staticmethod
    def read_stops(input_dir):
        """
        Read the stops from the input file in *input_dir*.
        """
        pandas.set_option('display.width', 1000)
        stops_df = pandas.read_csv(os.path.join(input_dir, Stop.INPUT_STOPS_FILE), sep="\t")
        FastTripsLogger.debug("=========== STOPS ===========\n" + str(stops_df.head()))
        FastTripsLogger.debug("\n"+str(stops_df.dtypes))

        stop_id_to_stop = {}
        stop_records = stops_df.to_dict(orient='records')
        for stop_record in stop_records:
            stop = Stop(stop_record)
            stop_id_to_stop[stop.stop_id] = stop

        FastTripsLogger.info("Read %7d stops" % len(stop_id_to_stop))
        return stop_id_to_stop

    @staticmethod
    def read_transfers(input_dir, stop_id_to_stop):
        """
        Read the transfers from the input file in *input_dir*.
        """
        transfers_df = pandas.read_csv(os.path.join(input_dir, Stop.INPUT_TRANSFERS_FILE), sep="\t")
        FastTripsLogger.debug("=========== TRANSFERS ===========\n" + str(transfers_df.head()))
        FastTripsLogger.debug("\n"+str(transfers_df.dtypes))

        transfer_records = transfers_df.to_dict(orient='records')
        for transfer_record in transfer_records:
            stop_id_to_stop[transfer_record['fromStop']].add_transfer(transfer_record)

        FastTripsLogger.info("Read %7d transfers" % len(transfers_df))
