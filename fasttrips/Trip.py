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
from .Route  import Route
from .Util   import Util

class Trip:
    """
    Trip class.

    One instance represents all of the transit vehicle trips.

    Stores Trip information in :py:attr:`Trip.trips_df`, an instance of :py:class:`pandas.DataFrame`
    and stop time information in :py:attr:`Trip.stop_times_df`, another instance of
    :py:class:`pandas.DataFrame`.

    Also stores Vehicle information in :py:attr:`Trips.vehicles_df` and
    Service Calendar information in :py:attr:`Trips.service_df`
    """

    #: File with fasttrips trip information (this extends the
    #: `gtfs trips <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/trips.md>`_ file).
    # See `trips_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/trips_ft.md>`_.
    INPUT_TRIPS_FILE                        = "trips_ft.txt"
    #: gtfs Trips column name: Unique identifier.  This will be the index of the trips table. (object)
    TRIPS_COLUMN_TRIP_ID                    = 'trip_id'
    #: gtfs Trips column name: Route unique identifier.
    TRIPS_COLUMN_ROUTE_ID                   = 'route_id'
    #: gtfs Trips column name: Service unique identifier.
    TRIPS_COLUMN_SERVICE_ID                 = 'service_id'
    #: gtfs Trip column name: Direction binary identifier.
    TRIPS_COLUMN_DIRECTION_ID               = 'direction_id'
    #: gtfs Trip column name: Shape ID
    TRIPS_COLUMN_SHAPE_ID                   = 'shape_id'

    #: fasttrips Trips column name: Vehicle Name
    TRIPS_COLUMN_VEHICLE_NAME               = 'vehicle_name'

    # ========== Added by fasttrips =======================================================
    #: fasttrips Trips column name: Trip Numerical Identifier. Int.
    TRIPS_COLUMN_TRIP_ID_NUM                = 'trip_id_num'
    #: fasttrips Trips column name: Route Numerical Identifier. Int.
    TRIPS_COLUMN_ROUTE_ID_NUM               = Route.ROUTES_COLUMN_ROUTE_ID_NUM
    #: fasttrips Trips column name: Mode Numerical Identifier. Int.
    TRIPS_COLUMN_MODE_NUM                   = Route.ROUTES_COLUMN_MODE_NUM

    #: File with fasttrips vehicles information.
    #: See `vehicles_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/vehicles_ft.md>`_.
    INPUT_VEHICLES_FILE                     = 'vehicles_ft.txt'
    #: fasttrips Vehicles column name: Vehicle name (identifier)
    VEHICLES_COLUMN_VEHICLE_NAME            = TRIPS_COLUMN_VEHICLE_NAME
    #: fasttrips Vehicles column name: Vehicle Description
    VEHICLES_COLUMN_VEHICLE_DESCRIPTION     = 'vehicle_description'
    #: fasttrips Vehicles column name: Seated Capacity
    VEHICLES_COLUMN_SEATED_CAPACITY         = 'seated_capacity'
    #: fasttrips Vehicles column name: Standing Capacity
    VEHICLES_COLUMN_STANDING_CAPACITY       = 'standing_capacity'
    #: fasttrips Vehicles column name: Number of Doors
    VEHICLES_COLUMN_NUMBER_OF_DOORS         = 'number_of_doors'
    #: fasttrips Vehicles column name: Maximum Speed (mph)
    VEHICLES_COLUMN_MAXIMUM_SPEED           = 'max_speed'
    #: fasttrips Vehicles column name: Vehicle Length (feet)
    VEHICLES_COLUMN_VEHICLE_LENGTH          = 'vehicle_length'
    #: fasttrips Vehicles column name: Platform Height (inches)
    VEHICLES_COLUMN_PLATFORM_HEIGHT         = 'platform_height'
    #: fasttrips Vehicles column name: Propulsion Type
    VEHICLES_COLUMN_PROPULSION_TYPE         = 'propulsion_type'
    #: fasttrips Vehicles column name: Wheelchair Capacity (overrides trip)
    VEHICLES_COLUMN_WHEELCHAIR_CAPACITY     = 'wheelchair_capacity'
    #: fasttrips Vehicles column name: Bicycle Capacity
    VEHICLES_COLUMN_BICYCLE_CAPACITY        = 'bicycle_capacity'

    # ========== Added by fasttrips =======================================================
    #: fasttrips Trips column name: Vehicle Total (Seated + Standing) Capacity
    VEHICLES_COLUMN_TOTAL_CAPACITY          = 'capacity'

    #: fasttrips Service column name: Start Date string in 'YYYYMMDD' format
    SERVICE_COLUMN_START_DATE_STR           = 'start_date_str'
    #: fasttrips Service column name: Start Date as datetime.date
    SERVICE_COLUMN_START_DATE               = 'start_date'
    #: fasttrips Service column name: End Date string in 'YYYYMMDD' format
    SERVICE_COLUMN_END_DATE_STR             = 'end_date_str'
    #: fasttrips Service column name: End Date as datetime.date
    SERVICE_COLUMN_END_DATE                 = 'end_date'

    #: File with fasttrips stop time information (this extends the
    #: `gtfs stop times <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/stop_times.md>`_ file).
    # See `stop_times_ft specification <https://github.com/osplanning-data-standards/GTFS-PLUS/blob/master/files/stop_times_ft.md>`_.
    INPUT_STOPTIMES_FILE                    = "stop_times_ft.txt"

    #: gtfs Stop times column name: Trip unique identifier. (String)
    STOPTIMES_COLUMN_TRIP_ID                = 'trip_id'
    #: gtfs Stop times column name: Stop unique identifier
    STOPTIMES_COLUMN_STOP_ID                = 'stop_id'
    #: gtfs Stop times column name: Sequence number of stop within a trip.
    #: Starts at 1 and is sequential
    STOPTIMES_COLUMN_STOP_SEQUENCE          = 'stop_sequence'

    #: Stop times column name: Arrival time.  This is a float, minutes after midnight.
    STOPTIMES_COLUMN_ARRIVAL_TIME_MIN       = 'arrival_time_min'
    #: gtfs Stop times column name: Arrival time.  This is a DateTime.
    STOPTIMES_COLUMN_ARRIVAL_TIME           = 'arrival_time'
    #: Stop times column name: Departure time. This is a float, minutes after midnight.
    STOPTIMES_COLUMN_DEPARTURE_TIME_MIN     = 'departure_time_min'
    #: gtfs Stop times column name: Departure time. This is a DateTime.
    STOPTIMES_COLUMN_DEPARTURE_TIME         = 'departure_time'

    #: gtfs Stop times stop times column name: Stop Headsign
    STOPTIMES_COLUMN_HEADSIGN               = 'stop_headsign'
    #: gtfs Stop times stop times column name: Pickup Type
    STOPTIMES_COLUMN_PICKUP_TYPE            = 'pickup_type'
    #: gtfs Stop times stop times column name: Drop Off Type
    STOPTIMES_COLUMN_DROP_OFF_TYPE          = 'drop_off_type'
    #: gtfs Stop times stop times column name: Shape Distance Traveled
    STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED    = 'shape_dist_traveled'
    #: gtfs Stop times stop times column name: Time Point
    STOPTIMES_COLUMN_TIMEPOINT              = 'timepoint'

    # ========== Added by fasttrips =======================================================
    #: fasttrips Trips column name: Trip Numerical Identifier. Int.
    STOPTIMES_COLUMN_TRIP_ID_NUM                = TRIPS_COLUMN_TRIP_ID_NUM
    #: fasttrips Trips column name: Stop Numerical Identifier. Int.
    STOPTIMES_COLUMN_STOP_ID_NUM                = 'stop_id_num'

    #: File with trip ID, trip ID number correspondence
    OUTPUT_TRIP_ID_NUM_FILE                     = 'ft_intermediate_trip_id.txt'
    #: File with trip information
    OUTPUT_TRIPINFO_FILE                        = 'ft_intermediate_trip_info.txt'

    #: Default headway if no previous matching route/trip
    DEFAULT_HEADWAY             = 60

    def __init__(self, input_dir, output_dir, gtfs_schedule, today, is_child_process, stops, routes, prepend_route_id_to_trip_id):
        """
        Constructor. Read the gtfs data from the transitfeed schedule, and the additional
        fast-trips stops data from the input files in *input_dir*.
        """
        self.output_dir = output_dir

        # Read vehicles first
        self.vehicles_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_VEHICLES_FILE))
        # verify the required columns are present
        vehicle_ft_cols = list(self.vehicles_df.columns.values)
        assert(Trip.VEHICLES_COLUMN_VEHICLE_NAME    in vehicle_ft_cols)

        if (Trip.VEHICLES_COLUMN_SEATED_CAPACITY   in vehicle_ft_cols and
            Trip.VEHICLES_COLUMN_STANDING_CAPACITY in vehicle_ft_cols):
            self.vehicles_df[Trip.VEHICLES_COLUMN_TOTAL_CAPACITY] = \
                self.vehicles_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY] + \
                self.vehicles_df[Trip.VEHICLES_COLUMN_STANDING_CAPACITY]
            self.capacity_configured = True
        else:
            self.capacity_configured = False

        FastTripsLogger.debug("=========== VEHICLES ===========\n" + str(self.vehicles_df.head()))
        FastTripsLogger.debug("\n"+str(self.vehicles_df.index.dtype)+"\n"+str(self.vehicles_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.vehicles_df), "vehicles", self.INPUT_VEHICLES_FILE))

        # Combine all gtfs Trip objects to a single pandas DataFrame
        trip_dicts      = []
        stop_time_dicts = []
        for gtfs_trip in gtfs_schedule.GetTripList():
            trip_dict = {}
            for fieldname in gtfs_trip._FIELD_NAMES:
                if fieldname in gtfs_trip.__dict__:
                    trip_dict[fieldname] = gtfs_trip.__dict__[fieldname]
            trip_dicts.append(trip_dict)

            # stop times
            #   _REQUIRED_FIELD_NAMES = ['trip_id', 'arrival_time', 'departure_time',
            #                            'stop_id', 'stop_sequence']
            #   _OPTIONAL_FIELD_NAMES = ['stop_headsign', 'pickup_type',
            #                            'drop_off_type', 'shape_dist_traveled', 'timepoint']
            for gtfs_stop_time in gtfs_trip.GetStopTimes():
                stop_time_dict = {}
                stop_time_dict[Trip.STOPTIMES_COLUMN_TRIP_ID]         = gtfs_trip.__dict__[Trip.STOPTIMES_COLUMN_TRIP_ID]
                stop_time_dict[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]    = gtfs_stop_time.arrival_time
                stop_time_dict[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]  = gtfs_stop_time.departure_time
                stop_time_dict[Trip.STOPTIMES_COLUMN_STOP_ID]         = gtfs_stop_time.stop_id
                stop_time_dict[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]   = gtfs_stop_time.stop_sequence
                # optional fields
                try:
                    stop_time_dict[Trip.STOPTIMES_COLUMN_HEADSIGN]            = gtfs_stop_time.stop_headsign
                except:
                    pass
                try:
                    stop_time_dict[Trip.STOPTIMES_COLUMN_PICKUP_TYPE]         = gtfs_stop_time.pickup_type
                except:
                    pass
                try:
                    stop_time_dict[Trip.STOPTIMES_COLUMN_DROP_OFF_TYPE]        = gtfs_stop_time.drop_off_type
                except:
                    pass
                try:
                    stop_time_dict[Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED] = gtfs_stop_time.shape_dist_traveled
                except:
                    pass
                try:
                    stop_time_dict[Trip.STOPTIMES_COLUMN_TIMEPOINT]           = gtfs_stop_time.timepoint
                except:
                    pass
                stop_time_dicts.append(stop_time_dict)

        self.trips_df = pandas.DataFrame(data=trip_dicts)

        # Read the fast-trips supplemental trips data file.  Make sure trip ID is read as a string.
        trips_ft_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_TRIPS_FILE),
                                      dtype={Trip.TRIPS_COLUMN_TRIP_ID:object})
        # verify required columns are present
        trips_ft_cols = list(trips_ft_df.columns.values)
        assert(Trip.TRIPS_COLUMN_TRIP_ID        in trips_ft_cols)
        assert(Trip.TRIPS_COLUMN_VEHICLE_NAME   in trips_ft_cols)

        # Join to the trips dataframe
        self.trips_df = pandas.merge(left=self.trips_df, right=trips_ft_df,
                                      how='left',
                                      on=Trip.TRIPS_COLUMN_TRIP_ID)


        # Trip IDs are strings. Create a unique numeric trip ID.
        self.trip_id_df = Util.add_numeric_column(self.trips_df[[Trip.TRIPS_COLUMN_TRIP_ID]],
                                                  id_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                                                  numeric_newcolname=Trip.TRIPS_COLUMN_TRIP_ID_NUM)
        FastTripsLogger.debug("Trip ID to number correspondence\n" + str(self.trip_id_df.head()))
        if not is_child_process:
            # prepend_route_id_to_trip_id
            if prepend_route_id_to_trip_id:
                # get the route id back again
                trip_id_df = pandas.merge(self.trip_id_df, self.trips_df[[Trip.TRIPS_COLUMN_TRIP_ID, Trip.TRIPS_COLUMN_ROUTE_ID]],
                                          how='left', on=Trip.TRIPS_COLUMN_TRIP_ID)
                trip_id_df.rename(columns={Trip.TRIPS_COLUMN_TRIP_ID: 'trip_id_orig'}, inplace=True)
                trip_id_df[Trip.TRIPS_COLUMN_TRIP_ID] = trip_id_df[Trip.TRIPS_COLUMN_ROUTE_ID].map(str) + str("_") + trip_id_df['trip_id_orig']
            else:
                trip_id_df = self.trip_id_df

            trip_id_df.to_csv(os.path.join(output_dir, Trip.OUTPUT_TRIP_ID_NUM_FILE),
                                   columns=[Trip.TRIPS_COLUMN_TRIP_ID_NUM, Trip.TRIPS_COLUMN_TRIP_ID],
                                   sep=" ", index=False)
            FastTripsLogger.debug("Wrote %s" % os.path.join(output_dir, Trip.OUTPUT_TRIP_ID_NUM_FILE))

        self.trips_df = pandas.merge(left=self.trips_df, right=self.trip_id_df, how='left')

        # Merge vehicles
        self.trips_df = pandas.merge(left=self.trips_df, right=self.vehicles_df, how='left')

        FastTripsLogger.debug("=========== TRIPS ===========\n" + str(self.trips_df.head()))
        FastTripsLogger.debug("\n"+str(self.trips_df.index.dtype)+"\n"+str(self.trips_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.trips_df), "trips", "trips.txt", self.INPUT_TRIPS_FILE))

        service_dicts = []
        for gtfs_service in gtfs_schedule.GetServicePeriodList():
            service_dict = {}
            service_tuple = gtfs_service.GetCalendarFieldValuesTuple()
            for fieldnum in range(len(gtfs_service._FIELD_NAMES)):
                # all required
                fieldname = gtfs_service._FIELD_NAMES[fieldnum]
                service_dict[fieldname] = service_tuple[fieldnum]
            service_dicts.append(service_dict)
        self.service_df = pandas.DataFrame(data=service_dicts)

        # Rename SERVICE_COLUMN_START_DATE to SERVICE_COLUMN_START_DATE_STR
        self.service_df[Trip.SERVICE_COLUMN_START_DATE_STR] = self.service_df[Trip.SERVICE_COLUMN_START_DATE]
        self.service_df[Trip.SERVICE_COLUMN_END_DATE_STR  ] = self.service_df[Trip.SERVICE_COLUMN_END_DATE  ]

        # Convert to datetime
        self.service_df[Trip.SERVICE_COLUMN_START_DATE] = \
            self.service_df[Trip.SERVICE_COLUMN_START_DATE_STR].map(lambda x: \
            datetime.datetime.combine(datetime.datetime.strptime(x, '%Y%M%d').date(), datetime.time(minute=0)))
        self.service_df[Trip.SERVICE_COLUMN_END_DATE] = \
            self.service_df[Trip.SERVICE_COLUMN_END_DATE_STR].map(lambda x: \
            datetime.datetime.combine(datetime.datetime.strptime(x, '%Y%M%d').date(), datetime.time(hour=23, minute=59, second=59, microsecond=999999)))

        # Join with routes
        self.trips_df = pandas.merge(left=self.trips_df, right=routes.routes_df,
                                     how='left',
                                     on=Trip.TRIPS_COLUMN_ROUTE_ID)
        FastTripsLogger.debug("Final\n"+str(self.trips_df.head()))
        FastTripsLogger.debug("\n"+str(self.trips_df.dtypes))

        FastTripsLogger.debug("=========== SERVICE PERIODS ===========\n" + str(self.service_df.head()))
        FastTripsLogger.debug("\n"+str(self.service_df.index.dtype)+"\n"+str(self.service_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.service_df), "service periods", "calendar.txt"))

        self.stop_times_df = pandas.DataFrame(data=stop_time_dicts)

        # Read the fast-trips supplemental stop times data file
        stop_times_ft_df = pandas.read_csv(os.path.join(input_dir, Trip.INPUT_STOPTIMES_FILE),
                                      dtype={Trip.STOPTIMES_COLUMN_TRIP_ID:object,
                                             Trip.STOPTIMES_COLUMN_STOP_ID:object})
        # verify required columns are present
        stop_times_ft_cols = list(stop_times_ft_df.columns.values)
        assert(Trip.STOPTIMES_COLUMN_TRIP_ID    in stop_times_ft_cols)
        assert(Trip.STOPTIMES_COLUMN_STOP_ID    in stop_times_ft_cols)

        # Join to the trips dataframe
        if len(stop_times_ft_cols) > 2:
            self.stop_times_df = pandas.merge(left=stop_times_df, right=stop_times_ft_df,
                                              how='left',
                                              on=[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                  Trip.STOPTIMES_COLUMN_STOP_ID])

        FastTripsLogger.debug("=========== STOP TIMES ===========\n" + str(self.stop_times_df.head()))
        FastTripsLogger.debug("\n"+str(self.stop_times_df.index.dtype)+"\n"+str(self.stop_times_df.dtypes))

        # datetime version
        self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].map(lambda x: Util.read_time(x))
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].map(lambda x: Util.read_time(x))

        # float version
        self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + x.time().second/60.0 )
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + x.time().second/60.0 )

        # skipping index setting for now -- it's annoying for joins
        # self.stop_times_df.set_index([Trip.STOPTIMES_COLUMN_TRIP_ID,
        #                              Trip.STOPTIMES_COLUMN_STOP_SEQUENCE], inplace=True, verify_integrity=True)

        # Add numeric stop and trip ids
        self.stop_times_df = stops.add_numeric_stop_id(self.stop_times_df,
                                                       id_colname=Trip.STOPTIMES_COLUMN_STOP_ID,
                                                       numeric_newcolname=Trip.STOPTIMES_COLUMN_STOP_ID_NUM)
        self.stop_times_df = self.add_numeric_trip_id(self.stop_times_df,
                                                      id_colname=Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                      numeric_newcolname=Trip.STOPTIMES_COLUMN_TRIP_ID_NUM)

        self.stop_times_df = Util.remove_null_columns(self.stop_times_df)

        FastTripsLogger.debug("Final\n" + str(self.stop_times_df.head().to_string(formatters=\
                              {Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:Util.datetime64_formatter,
                               Trip.STOPTIMES_COLUMN_ARRIVAL_TIME  :Util.datetime64_formatter})) + \
                              "\n" +str(self.stop_times_df.dtypes) )
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.stop_times_df), "stop times", "stop_times.txt", Trip.INPUT_STOPTIMES_FILE))


        if not is_child_process:
            self.write_trips_for_extension()

    def has_capacity_configured(self):
        """
        Returns true if seated capacity and standing capacity are columns included in the vehicles input.
        """
        return self.capacity_configured

    def add_numeric_trip_id(self, input_df, id_colname, numeric_newcolname):
        """
        Passing a :py:class:`pandas.DataFrame` with a trip ID column called *id_colname*,
        adds the numeric trip id as a column named *numeric_newcolname* and returns it.
        """
        return Util.add_new_id(input_df, id_colname, numeric_newcolname,
                                   mapping_df=self.trip_id_df,
                                   mapping_id_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                                   mapping_newid_colname=Trip.TRIPS_COLUMN_TRIP_ID_NUM)

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

    def write_trips_for_extension(self):
        """
        This writes to an intermediate file a formatted file for the C++ extension.
        Since there are strings involved, it's easier than passing it to the extension.
        """
        trips_df = self.trips_df.copy()

        # drop some of the attributes
        drop_fields = [Trip.TRIPS_COLUMN_TRIP_ID,             # use numerical version
                       Trip.TRIPS_COLUMN_ROUTE_ID,            # use numerical version
                       Trip.TRIPS_COLUMN_SERVICE_ID,          # I don't think this is useful
                       Trip.TRIPS_COLUMN_DIRECTION_ID,        # I don't think this is useful
                       Trip.TRIPS_COLUMN_VEHICLE_NAME,        # could pass numerical version
                       Trip.TRIPS_COLUMN_SHAPE_ID,            # I don't think this is useful
                       Route.ROUTES_COLUMN_MODE_TYPE,         # I don't think this is useful -- should be transit
                       Route.ROUTES_COLUMN_ROUTE_SHORT_NAME,  # I don't think this is useful
                       Route.ROUTES_COLUMN_ROUTE_LONG_NAME,   # I don't think this is useful
                       Route.ROUTES_COLUMN_ROUTE_TYPE,        # I don't think this is useful
                       Route.ROUTES_COLUMN_MODE,              # use numerical version
                       Route.ROUTES_COLUMN_FARE_CLASS,        # text
                       Route.ROUTES_COLUMN_PROOF_OF_PAYMENT,  # text
                       ]
        # we can only drop fields that are in the dataframe
        trip_fields = list(trips_df.columns.values)
        valid_drop_fields = []
        for field in drop_fields:
            if field in trip_fields: valid_drop_fields.append(field)

        trips_df.drop(valid_drop_fields, axis=1, inplace=1)

        # only pass on numeric columns -- for now, drop the rest
        FastTripsLogger.debug("Dropping non-numeric trip info")
        FastTripsLogger.debug(str(trips_df.head()))
        trips_df = trips_df.select_dtypes(exclude=['object'])
        FastTripsLogger.debug(str(trips_df.head()))

        # the index is the trip_id_num
        trips_df.set_index(Trip.TRIPS_COLUMN_TRIP_ID_NUM, inplace=True)
        # this will make it so beyond trip id num
        # the remaining columns collapse to variable name, variable value
        trips_df = trips_df.stack().reset_index()
        trips_df.rename(columns={"level_1":"attr_name", 0:"attr_value"}, inplace=True)

        trips_df.to_csv(os.path.join(self.output_dir, Trip.OUTPUT_TRIPINFO_FILE),
                        sep=" ", index=False)
        FastTripsLogger.debug("Wrote %s" % os.path.join(self.output_dir, Trip.OUTPUT_TRIPINFO_FILE))

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
        # trips_df.loc[trips_df.service_type==0, 'dwell_time']                   = 30

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
        # what if direction_id isn't specified
        has_direction_id = Trip.TRIPS_COLUMN_DIRECTION_ID in trips_df.columns.values

        if has_direction_id:
            stop_group = trips_df[[Trip.STOPTIMES_COLUMN_STOP_ID,
                                   Trip.TRIPS_COLUMN_ROUTE_ID,
                                   Trip.TRIPS_COLUMN_DIRECTION_ID,
                                   Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,
                                   Trip.STOPTIMES_COLUMN_TRIP_ID,
                                   Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]].groupby([Trip.STOPTIMES_COLUMN_STOP_ID,
                                                                                 Trip.TRIPS_COLUMN_ROUTE_ID,
                                                                                 Trip.TRIPS_COLUMN_DIRECTION_ID])
        else:
            stop_group = trips_df[[Trip.STOPTIMES_COLUMN_STOP_ID,
                                   Trip.TRIPS_COLUMN_ROUTE_ID,
                                   Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,
                                   Trip.STOPTIMES_COLUMN_TRIP_ID,
                                   Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]].groupby([Trip.STOPTIMES_COLUMN_STOP_ID,
                                                                                 Trip.TRIPS_COLUMN_ROUTE_ID])

        stop_group_df = stop_group.apply(lambda x: x.sort(Trip.STOPTIMES_COLUMN_DEPARTURE_TIME))
        # set headway, in minutes
        stop_group_shift_df = stop_group_df.shift()
        stop_group_df['headway'] = (stop_group_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] - stop_group_shift_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME])/numpy.timedelta64(1,'m')
        # zero out the first in each group
        if has_direction_id:
            stop_group_df.loc[(stop_group_df.stop_id     !=stop_group_shift_df.stop_id     )|
                              (stop_group_df.route_id    !=stop_group_shift_df.route_id    )|
                              (stop_group_df.direction_id!=stop_group_shift_df.direction_id), 'headway'] = Trip.DEFAULT_HEADWAY
        else:
            stop_group_df.loc[(stop_group_df.stop_id     !=stop_group_shift_df.stop_id     )|
                              (stop_group_df.route_id    !=stop_group_shift_df.route_id    ), 'headway'] = Trip.DEFAULT_HEADWAY
        # print stop_group_df

        trips_df_len = len(trips_df)
        trips_df = pandas.merge(left  = trips_df,
                                right = stop_group_df[[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                       Trip.STOPTIMES_COLUMN_STOP_ID,
                                                       Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                       'headway']],
                                on    = [Trip.STOPTIMES_COLUMN_TRIP_ID,
                                         Trip.STOPTIMES_COLUMN_STOP_ID,
                                         Trip.STOPTIMES_COLUMN_STOP_SEQUENCE])
        assert(len(trips_df)==trips_df_len)
        return trips_df
