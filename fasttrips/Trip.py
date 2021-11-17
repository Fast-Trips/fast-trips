from __future__ import division
from builtins import str
from builtins import object

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
import datetime
import os

import numpy as np
import pandas as pd

from .Error  import NetworkInputError
from .Logger import FastTripsLogger
from .Route  import Route
from .Util   import Util


class Trip(object):
    """
    Trip class.

    One instance represents all of the transit vehicle trips.

    Stores Trip information in :py:attr:`Trip.trips_df`, an instance of :py:class:`pandas.DataFrame`
    and stop time information in :py:attr:`Trip.stop_times_df`, another instance of
    :py:class:`pandas.DataFrame`.

    Also stores Vehicle information in :py:attr:`Trip.vehicles_df` and
    Service Calendar information in :py:attr:`Trip.service_df`
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
    #: fasttrips Trips column name: Max Stop Sequence number. Int.
    TRIPS_COLUMN_MAX_STOP_SEQUENCE          = 'max_stop_seq'
    #: fasttrips Trip column name: Trip departure time (from the first stop).
    TRIPS_COLUMN_TRIP_DEPARTURE_TIME         = 'trip_departure_time'

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
    #: fasttrips Vehicles column name: Acceleration (feet per (second^2))  float.
    VEHICLES_COLUMN_ACCELERATION            = 'acceleration'
    #: fasttrips Vehicles column name: Decelration (feet per (second^2))  float.
    VEHICLES_COLUMN_DECELERATION            = 'deceleration'
    #: fasttrips Vehilces column name: Dwell formula (string)
    VEHICLES_COLUMN_DWELL_FORMULA           = 'dwell_formula'

    # ========== Added by fasttrips =======================================================
    #: fasttrips Trips column name: Vehicle Total (Seated + Standing) Capacity
    VEHICLES_COLUMN_TOTAL_CAPACITY          = 'capacity'
    #: fasttrips Vehicles column name: Maximum Speed (fps)
    VEHICLES_COLUMN_MAXIMUM_SPEED_FPS       = 'max_speed_fps'

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
    #: fasttrips Trips column name: Original Travel Time.  This is a timedelta.
    #: This represents the travel from original input, and is assumed to include
    #: accel from first stop and decel to last stop, but no dwell times and no other
    #: accel/decel times.
    STOPTIMES_COLUMN_ORIGINAL_TRAVEL_TIME       = "original_travel_time"
    #: fasttrips Trips column name: Travel Time.  This is a timedelta.
    STOPTIMES_COLUMN_TRAVEL_TIME                = "travel_time"
    #: fasttrips Trips column name: Travel Time in seconds. Float.
    STOPTIMES_COLUMN_TRAVEL_TIME_SEC            = "travel_time_sec"
    #: fasttrips Trips column name: Dwell Time.  This is a timedelta.
    STOPTIMES_COLUMN_DWELL_TIME                 = "dwell_time"
    #: fasttrips Trips column name: Dwell Time in seconds.  Float
    STOPTIMES_COLUMN_DWELL_TIME_SEC             = "dwell_time_sec"


    #: File with trip ID, trip ID number correspondence
    OUTPUT_TRIP_ID_NUM_FILE                     = 'ft_intermediate_trip_id.txt'
    #: File with trip information
    OUTPUT_TRIPINFO_FILE                        = 'ft_intermediate_trip_info.txt'

    #: Default headway if no previous matching route/trip
    DEFAULT_HEADWAY             = 60

    # ========== Simulation column names =======================================================
    #: Result column name: Boards. Int.
    SIM_COL_VEH_BOARDS                          = 'boards'
    #: Result column name: Alights.  Int.
    SIM_COL_VEH_ALIGHTS                         = 'alights'
    #: Result column name: Onboard.  Cumulative sum of :py:attr:`SIM_COL_VEH_BOARDS` - :py:attr:`SIM_COL_VEH_ALIGHTS`. Int.
    SIM_COL_VEH_ONBOARD                         = 'onboard'
    #: Result column name: Standees. Onboard - sitting capacity, if configured. Int.
    SIM_COL_VEH_STANDEES                        = 'standees'
    #: Result column name: Friction. Where positive, friction = on+off+standees. Int.
    SIM_COL_VEH_FRICTION                        = 'friction'
    #: Result column name: Number of onboard passengers minus capacity. Int. Can be negative.
    SIM_COL_VEH_OVERCAP                         = 'overcap'
    #: Result column name: Percentage of boards that are problematic. (overcap/boards)
    SIM_COL_VEH_OVERCAP_FRAC                    = 'overcap_frac'

    #: Result column name: MSA of column :py:attr:`SIM_COL_VEH_BOARDS`. Float.
    SIM_COL_VEH_MSA_BOARDS                      = 'msa_boards'
    #: Result column name: MSA of column :py:attr:`SIM_COL_VEH_ALIGHTS`. Float.
    SIM_COL_VEH_MSA_ALIGHTS                     = 'msa_alights'
    #: Result column name: MSA Onboard. Cumulative sum of :py:attr:`SIM_COL_VEH_MSA_BOARDS` - :py:attr:`SIM_COL_VEH_MSA_ALIGHTS`. Float.
    SIM_COL_VEH_MSA_ONBOARD                     = 'msa_onboard'
    #: Result column name: MSA Standeeds.  MSA onboard - sitting capacity, if configured. Float.
    SIM_COL_VEH_MSA_STANDEES                    = 'msa_standees'
    #: Result column name: MSA Friction. Where positive, MSA friction = MSA boards + MSA alights + MSA standees. Float.
    SIM_COL_VEH_MSA_FRICTION                    = 'msa_friction'
    #: Result column name: Number of MSA onboard passengers minus capacity. Float.
    SIM_COL_VEH_MSA_OVERCAP                     = 'msa_overcap'

    def __init__(self, input_archive, output_dir, gtfs_feed, today, stops, routes, prepend_route_id_to_trip_id):
        """
        Constructor. Read the gtfs data from the transitfeed schedule, and the additional
        fast-trips stops data from the input files in *input_archive*.
        """
        self.output_dir = output_dir

        # Read vehicles first
        self.vehicles_df = gtfs_feed.get(Trip.INPUT_VEHICLES_FILE)
        trips_ft_df = gtfs_feed.get(Trip.INPUT_TRIPS_FILE)

        # verify the required columns are present
        assert(Trip.VEHICLES_COLUMN_VEHICLE_NAME in self.vehicles_df)

        if ({Trip.VEHICLES_COLUMN_SEATED_CAPACITY, Trip.VEHICLES_COLUMN_STANDING_CAPACITY}.issubset(self.vehicles_df)):
            self.vehicles_df[Trip.VEHICLES_COLUMN_TOTAL_CAPACITY] = \
                self.vehicles_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY] + \
                self.vehicles_df[Trip.VEHICLES_COLUMN_STANDING_CAPACITY]
            self.capacity_configured = True
        else:
            self.capacity_configured = False

            # error if capacity constraints are on
            from .Assignment import Assignment
            if Assignment.CAPACITY_CONSTRAINT:
                error_str = "capacity_constraint is configured to be True but either %s or %s is missing from vehicle information" % \
                    (Trip.VEHICLES_COLUMN_SEATED_CAPACITY, Trip.VEHICLES_COLUMN_STANDING_CAPACITY)
                FastTripsLogger.fatal(error_str)

                raise NetworkInputError(Trip.INPUT_VEHICLES_FILE, error_str)

        # convert mph to fps for maximum speed
        if Trip.VEHICLES_COLUMN_MAXIMUM_SPEED in self.vehicles_df:
            self.vehicles_df[Trip.VEHICLES_COLUMN_MAXIMUM_SPEED_FPS] = \
                self.vehicles_df[Trip.VEHICLES_COLUMN_MAXIMUM_SPEED]*5280.0/(60.0*60.0)

        FastTripsLogger.debug("=========== VEHICLES ===========\n" + str(self.vehicles_df.head()))
        FastTripsLogger.debug("\n"+str(self.vehicles_df.index.dtype)+"\n"+str(self.vehicles_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s" %
                             (len(self.vehicles_df), "vehicles", self.INPUT_VEHICLES_FILE))

        # Combine all gtfs Trip objects to a single pandas DataFrame
        self.trips_df = gtfs_feed.trips
        if len(self.trips_df) == 0:
            error_str = "No GTFS trips found for simulation day. Simulation day=%s" % (today.strftime("%B %d, %Y"))
            FastTripsLogger.fatal(error_str)

            raise NetworkInputError(Trip.INPUT_TRIPS_FILE, error_str)


        # verify required columns are present
        assert({Trip.TRIPS_COLUMN_TRIP_ID,Trip.TRIPS_COLUMN_VEHICLE_NAME}.issubset(trips_ft_df))

        # Join to the trips dataframe
        self.trips_df = pd.merge(left=self.trips_df, right=trips_ft_df,
                                      how='left',
                                      on=Trip.TRIPS_COLUMN_TRIP_ID)


        # Trip IDs are strings. Create a unique numeric trip ID.
        self.trip_id_df = Util.add_numeric_column(self.trips_df[[Trip.TRIPS_COLUMN_TRIP_ID]],
                                                  id_colname=Trip.TRIPS_COLUMN_TRIP_ID,
                                                  numeric_newcolname=Trip.TRIPS_COLUMN_TRIP_ID_NUM)
        FastTripsLogger.debug("Trip ID to number correspondence\n" + str(self.trip_id_df.head()))

        # prepend_route_id_to_trip_id
        if prepend_route_id_to_trip_id:
            # get the route id back again
            trip_id_df = pd.merge(self.trip_id_df, self.trips_df[[Trip.TRIPS_COLUMN_TRIP_ID, Trip.TRIPS_COLUMN_ROUTE_ID]],
                                      how='left', on=Trip.TRIPS_COLUMN_TRIP_ID)
            trip_id_df.rename(columns={Trip.TRIPS_COLUMN_TRIP_ID: 'trip_id_orig'}, inplace=True)
            trip_id_df[Trip.TRIPS_COLUMN_TRIP_ID] = trip_id_df[Trip.TRIPS_COLUMN_ROUTE_ID].map(str) + str("_") + trip_id_df['trip_id_orig']
        else:
            trip_id_df = self.trip_id_df

        trip_id_df.to_csv(os.path.join(output_dir, Trip.OUTPUT_TRIP_ID_NUM_FILE),
                               columns=[Trip.TRIPS_COLUMN_TRIP_ID_NUM, Trip.TRIPS_COLUMN_TRIP_ID],
                               sep=" ", index=False)
        FastTripsLogger.debug("Wrote %s" % os.path.join(output_dir, Trip.OUTPUT_TRIP_ID_NUM_FILE))

        self.trips_df = pd.merge(left=self.trips_df, right=self.trip_id_df, on=Trip.TRIPS_COLUMN_TRIP_ID, how='left')

        # Merge vehicles
        self.trips_df = pd.merge(left=self.trips_df, right=self.vehicles_df, how='left',
                                     left_on=Trip.TRIPS_COLUMN_VEHICLE_NAME, right_on=Trip.VEHICLES_COLUMN_VEHICLE_NAME)

        FastTripsLogger.debug("=========== TRIPS ===========\n" + str(self.trips_df.head()))
        FastTripsLogger.debug("\n"+str(self.trips_df.index.dtype)+"\n"+str(self.trips_df.dtypes))
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.trips_df), "trips", "trips.txt", self.INPUT_TRIPS_FILE))

        # Join with routes
        self.trips_df = pd.merge(left=self.trips_df, right=routes.routes_df,
                                     how='left',
                                     on=Trip.TRIPS_COLUMN_ROUTE_ID)
        FastTripsLogger.debug("Final (%d)\n%s" % (len(self.trips_df), str(self.trips_df.head())))
        FastTripsLogger.debug("\n"+str(self.trips_df.dtypes))

        self.stop_times_df = gtfs_feed.stop_times

        # Read the fast-trips supplemental stop times data file
        stop_times_ft_df = gtfs_feed.get(Trip.INPUT_STOPTIMES_FILE)
        if not stop_times_ft_df.empty:
            # verify required columns are present
            stop_times_ft_cols = list(stop_times_ft_df)
            assert({Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_STOP_ID}.issubset(stop_times_ft_df))

            # Join to the trips dataframe
            if len(stop_times_ft_df.columns) > 2:
                self.stop_times_df = pd.merge(left=self.stop_times_df, right=stop_times_ft_df,
                                                  how='left',
                                                  on=[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                      Trip.STOPTIMES_COLUMN_STOP_ID])

        FastTripsLogger.debug("=========== STOP TIMES ===========\n" + str(self.stop_times_df.head()))
        FastTripsLogger.debug("\n"+str(self.stop_times_df.index.dtype)+"\n"+str(self.stop_times_df.dtypes))

        self.add_shape_dist_traveled(stops)

        self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME] = self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]/ 60
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] = self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]/ 60
        self.stop_times_df.rename(columns={
            Trip.STOPTIMES_COLUMN_ARRIVAL_TIME: Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN,
            Trip.STOPTIMES_COLUMN_DEPARTURE_TIME: Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN,
        }, inplace=True)

        # datetime version
        self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN].map(lambda x: Util.parse_minutes_to_time(x))
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN].map(lambda x: Util.parse_minutes_to_time(x))

        # Add numeric stop and trip ids
        self.stop_times_df = stops.add_numeric_stop_id(self.stop_times_df,
                                                       id_colname=Trip.STOPTIMES_COLUMN_STOP_ID,
                                                       numeric_newcolname=Trip.STOPTIMES_COLUMN_STOP_ID_NUM)
        self.stop_times_df = self.add_numeric_trip_id(self.stop_times_df,
                                                      id_colname=Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                      numeric_newcolname=Trip.STOPTIMES_COLUMN_TRIP_ID_NUM)

        self.stop_times_df = Util.remove_null_columns(self.stop_times_df)

        self.add_original_travel_time_and_dwell()
        self.add_trip_attrs_from_stoptimes()

        FastTripsLogger.debug("Final\n" + str(self.stop_times_df.head().to_string(formatters=\
                              {Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:Util.datetime64_formatter,
                               Trip.STOPTIMES_COLUMN_ARRIVAL_TIME  :Util.datetime64_formatter})) + \
                              "\n" +str(self.stop_times_df.dtypes) )
        FastTripsLogger.info("Read %7d %15s from %25s, %25s" %
                             (len(self.stop_times_df), "stop times", "stop_times.txt", Trip.INPUT_STOPTIMES_FILE))

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
        return self.stop_times_df.loc[
            self.stop_times_df[Trip.STOPTIMES_COLUMN_TRIP_ID] == trip_id
        ]

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

    def add_original_travel_time_and_dwell(self):
        """
        Add original travel time to the :py:attr:`Trip.stop_times_df` as the
        column named :py:attr:`Trip.STOPTIMES_COLUMN_ORIGINAL_TRAVEL_TIME`

        Copies it into working travel time columns :py:attr:`Trip.STOPTIMES_COLUMN_TRAVEL_TIME`
        and :py:attr:`Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC`

        Also adds dwell time columns named :py:attr:`Trip.STOPTIMES_COLUMN_DWELL_TIME`
        and :py:attr:`Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC`
        """
        stop_times_len_df = len(self.stop_times_df)
        # first, find the original travel time following each stop
        # need to join the next stop and it's arrival time
        next_stop_df = self.stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                           Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                           Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]].copy()
        next_stop_df.loc[:,Trip.STOPTIMES_COLUMN_STOP_SEQUENCE] = next_stop_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]-1
        next_stop_df.rename(columns={Trip.STOPTIMES_COLUMN_ARRIVAL_TIME:"next_stop_arrival"}, inplace=True)

        FastTripsLogger.debug("next stop arrival:\n%s\n" % next_stop_df.head().to_string())

        self.stop_times_df = pd.merge(left=self.stop_times_df, right=next_stop_df, how='left')
        assert(stop_times_len_df == len(self.stop_times_df))

        # this will be NaT for last stops
        self.stop_times_df[Trip.STOPTIMES_COLUMN_ORIGINAL_TRAVEL_TIME] = \
            self.stop_times_df["next_stop_arrival"] - self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]
        # drop the extra column
        self.stop_times_df.drop(["next_stop_arrival"], axis=1, inplace=True)

        # copy
        self.stop_times_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME    ] = self.stop_times_df[Trip.STOPTIMES_COLUMN_ORIGINAL_TRAVEL_TIME]
        self.stop_times_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC] = \
            (self.stop_times_df[Trip.STOPTIMES_COLUMN_ORIGINAL_TRAVEL_TIME]/np.timedelta64(1, 's'))

        # dwell time
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DWELL_TIME] = \
            self.stop_times_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] - self.stop_times_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME]
        self.stop_times_df[Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC] = \
            (self.stop_times_df[Trip.STOPTIMES_COLUMN_DWELL_TIME]/np.timedelta64(1, 's'))

    def add_trip_attrs_from_stoptimes(self):
        """
        Adds the columns :py:attr:`Trip.TRIPS_COLUMN_MAX_STOP_SEQUENCE` and :py:attr:`Trip.TRIPS_COLUMN_TRIP_DEPARTURE_TIME`
        to the :py:attr:`Trip.trips_df` datatable to facilitate stop time updates in :py:meth:`Trip.update_trip_times`
        """
        trips_len = len(self.trips_df)

        stops_by_trip = self.stop_times_df.groupby(Trip.STOPTIMES_COLUMN_TRIP_ID_NUM).agg(
                            {Trip.STOPTIMES_COLUMN_STOP_SEQUENCE :'max',
                             Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:'min'}).reset_index()
        # rename it to max_stop_seq
        stops_by_trip.rename(columns={Trip.STOPTIMES_COLUMN_STOP_SEQUENCE :Trip.TRIPS_COLUMN_MAX_STOP_SEQUENCE,
                                      Trip.STOPTIMES_COLUMN_DEPARTURE_TIME:Trip.TRIPS_COLUMN_TRIP_DEPARTURE_TIME}, inplace=True)

        # add it to trips
        self.trips_df = pd.merge(left=self.trips_df, right=stops_by_trip)
        # make sure we didn't change the length
        assert(trips_len == len(self.trips_df))

    def add_shape_dist_traveled(self, stops):
        """
        For any trips in :py:attr:`Trip.stop_times_df` with rows that are missing column `shape_dist_traveled`,
        this method will fill in the column from the stop longitude & latitude.  *stops* is a :py:class:`Stop` instance.

        .. todo:: this will be in miles, but should be configurable

        """
        # temp column: null_shape_dist_traveled
        if Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED not in self.stop_times_df:
            self.stop_times_df[Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED] = np.nan

        self.stop_times_df["null_shape_dist_traveled"] = pd.isnull(self.stop_times_df[Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED])
        FastTripsLogger.debug("add_shape_dist_traveled: missing %d out of %d shape_dist_traveled values in the stop times dataframe" %
                              (self.stop_times_df["null_shape_dist_traveled"].sum(), len(self.stop_times_df["null_shape_dist_traveled"])))

        # if they're all set, nothing to do
        if self.stop_times_df["null_shape_dist_traveled"].sum() == 0:
            self.stop_times_df.drop(["null_shape_dist_traveled"], axis=1, inplace=True)
            return
        FastTripsLogger.warn("Adding shape_dist_traveled in miles")

        # aggregate to trip to find which trips are missing this field
        stop_times_trips = self.stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID, "null_shape_dist_traveled"]].groupby([Trip.STOPTIMES_COLUMN_TRIP_ID]).sum()
        stop_times_trips["null_shape_dist_traveled_bool"] = False
        stop_times_trips.loc[ stop_times_trips["null_shape_dist_traveled"] > 0, "null_shape_dist_traveled_bool"] = True

        stop_times_trips.drop(["null_shape_dist_traveled"], axis=1, inplace=True)
        stop_times_trips.reset_index(drop=False, inplace=True)
        # FastTripsLogger.debug("add_shape_dist_traveled() stop_times_trips\n%s" % str(stop_times_trips))

        # join this back to the stop_times dataframe
        self.stop_times_df.drop(["null_shape_dist_traveled"], axis=1, inplace=True)
        self.stop_times_df = pd.merge(left =self.stop_times_df,
                                          right=stop_times_trips,
                                          on   =Trip.STOPTIMES_COLUMN_TRIP_ID,
                                          how  ="left")

        # join stop information
        self.stop_times_df = stops.add_stop_lat_lon(self.stop_times_df, Trip.STOPTIMES_COLUMN_STOP_ID, "stop_lat", "stop_lon")

        # convert to links by adding prev_stop_seq -- this goes from 0 to max-1
        self.stop_times_df["stop_seq_prev"] = self.stop_times_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE] - 1
        self.stop_times_df = pd.merge(left    =self.stop_times_df,
                                          left_on =[Trip.STOPTIMES_COLUMN_TRIP_ID, "stop_seq_prev"],
                                          right   =self.stop_times_df[[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                                       Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                                       "stop_lat","stop_lon"]],
                                          right_on=[Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                          how     ="left",
                                          suffixes=["","_prev"])
        Util.calculate_distance_miles(self.stop_times_df, "stop_lat","stop_lon","stop_lat_prev","stop_lon_prev","calc shape_dist_traveled")

        # make it cumulative
        self.stop_times_df.loc[ (self.stop_times_df["null_shape_dist_traveled_bool"])&
                                (self.stop_times_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]==1), "calc shape_dist_traveled" ] = 0.0
        self.stop_times_df["calc shape_dist_traveled"] = self.stop_times_df.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID])["calc shape_dist_traveled"].apply(lambda x: x.cumsum())

        # incorporate it
        self.stop_times_df.loc[ (self.stop_times_df["null_shape_dist_traveled_bool"]), Trip.STOPTIMES_COLUMN_SHAPE_DIST_TRAVELED] = self.stop_times_df["calc shape_dist_traveled"]

        FastTripsLogger.debug("add_shape_dist_traveled() stop_times_df\n%s" % str(self.stop_times_df.head()))

        # drop the temp fields
        self.stop_times_df.drop(["null_shape_dist_traveled_bool",
                                 "stop_lat", "stop_lon",
                                 "stop_seq_prev", "stop_sequence_prev",
                                 "stop_lat_prev", "stop_lon_prev", "calc shape_dist_traveled"], axis=1, inplace=True)
        FastTripsLogger.debug("add_shape_dist_traveled() stop_times_df\n%s" % str(self.stop_times_df.head()))

    def get_full_trips(self):
        """

        Returns the fullest dataframe of trip + stop information.
        """
        # join with trips to get additional fields
        df = pd.merge(left = self.stop_times_df,
                          right= self.trips_df,
                          how  = 'left',
                          on   =[Trip.TRIPS_COLUMN_TRIP_ID, Trip.TRIPS_COLUMN_TRIP_ID_NUM])
        assert(len(self.stop_times_df) == len(df))

        # blank boards, alights and onboard
        df[Trip.SIM_COL_VEH_BOARDS      ] = 0
        df[Trip.SIM_COL_VEH_ALIGHTS     ] = 0
        df[Trip.SIM_COL_VEH_ONBOARD     ] = 0
        df[Trip.SIM_COL_VEH_FRICTION    ] = 0
        df[Trip.SIM_COL_VEH_STANDEES    ] = 0
        df[Trip.SIM_COL_VEH_OVERCAP     ] = -1 # assume there's room

        df[Trip.SIM_COL_VEH_MSA_BOARDS  ] = 0.0
        df[Trip.SIM_COL_VEH_MSA_ALIGHTS ] = 0.0
        df[Trip.SIM_COL_VEH_MSA_ONBOARD ] = 0.0
        df[Trip.SIM_COL_VEH_MSA_FRICTION] = 0.0
        df[Trip.SIM_COL_VEH_MSA_STANDEES] = 0.0
        df[Trip.SIM_COL_VEH_MSA_OVERCAP ] =-1.0 # assume there's room
        return df

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
                       Trip.TRIPS_COLUMN_MAX_STOP_SEQUENCE,   # I don't think this is useful
                       Trip.TRIPS_COLUMN_TRIP_DEPARTURE_TIME, # I don't think this is useful
                       Route.ROUTES_COLUMN_MODE_TYPE,         # I don't think this is useful -- should be transit
                       Route.ROUTES_COLUMN_ROUTE_SHORT_NAME,  # I don't think this is useful
                       Route.ROUTES_COLUMN_ROUTE_LONG_NAME,   # I don't think this is useful
                       Route.ROUTES_COLUMN_ROUTE_TYPE,        # I don't think this is useful
                       Route.ROUTES_COLUMN_MODE,              # use numerical version
                       Route.FARE_ATTR_COLUMN_FARE_PERIOD,    # text
                       Route.ROUTES_COLUMN_PROOF_OF_PAYMENT,  # text
                       ]
        # we can only drop fields that are in the dataframe
        trip_fields = list(trips_df.columns.values)
        valid_drop_fields = []
        for field in drop_fields:
            if field in trip_fields: valid_drop_fields.append(field)

        trips_df.drop(valid_drop_fields, axis=1, inplace=True)

        # only pass on numeric columns -- for now, drop the rest
        FastTripsLogger.debug("Dropping non-numeric trip info\n" + str(trips_df.head()))
        trips_df = trips_df.select_dtypes(exclude=['object'])
        FastTripsLogger.debug("\n"+str(trips_df.head()))

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
    def reset_onboard(df):
        """
        Resets the onboard fields for the given vehicle trip table.
        """
        df[Trip.SIM_COL_VEH_BOARDS      ] = 0
        df[Trip.SIM_COL_VEH_ALIGHTS     ] = 0
        df[Trip.SIM_COL_VEH_ONBOARD     ] = 0
        df[Trip.SIM_COL_VEH_FRICTION    ] = 0
        df[Trip.SIM_COL_VEH_STANDEES    ] = 0
        df[Trip.SIM_COL_VEH_OVERCAP     ] = -1 # assume there's room

        df[Trip.SIM_COL_VEH_MSA_BOARDS  ] = 0.0
        df[Trip.SIM_COL_VEH_MSA_ALIGHTS ] = 0.0
        df[Trip.SIM_COL_VEH_MSA_ONBOARD ] = 0.0
        df[Trip.SIM_COL_VEH_MSA_FRICTION] = 0.0
        df[Trip.SIM_COL_VEH_MSA_STANDEES] = 0.0
        df[Trip.SIM_COL_VEH_MSA_OVERCAP ] =-1.0 # assume there's room

    @staticmethod
    def update_trip_times(trips_df, MSA_RESULTS):
        """
        Updates trip times for stops with boards and/or alights.

        If vehicle max_speed and deceleration rate specified, for non first/last stop, adds lost time due to stopping.
        If vehicle max_speed and acceleration rate specified, for non first/last stop, adds lost time due to stopping.
        If dwell time formula specified, adds dwell time at stop.

        Updates the following columns:
        - Trip.SIM_COL_VEH_STANDEES
        - Trip.SIM_COL_VEH_MSA_STANDEES
        - Trip.SIM_COL_VEH_FRICTION
        - Trip.SIM_COL_VEH_MSA_FRICTION
        - Trip.STOPTIMES_COLUMN_TRAVEL_TIME
        - Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC
        - Trip.STOPTIMES_COLUMN_DWELL_TIME
        - Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC
        - Trip.STOPTIMES_COLUMN_ARRIVAL_TIME
        - Trip.STOPTIMES_COLUMN_DEPARTURE_TIME

        """
        trips_df_len = len(trips_df)
        FastTripsLogger.debug("Trip.update_trip_times() trips_df has %d rows" % len(trips_df))
        # FastTripsLogger.debug("trips_df.dtypes=\n%s\n" % str(trips_df.dtypes))
        trip_cols = list(trips_df.columns.values)

        # Default to 0
        trips_df[Trip.SIM_COL_VEH_FRICTION    ] = 0.0
        trips_df[Trip.SIM_COL_VEH_MSA_FRICTION] = 0.0

        if Trip.VEHICLES_COLUMN_SEATED_CAPACITY in trip_cols:

            # log null seated capacities
            if pd.isnull(trips_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY]).sum() > 0:
                FastTripsLogger.warn("Trip.update_trip_times(): some [%s] not configured; assuming zero friction for those vehicles" % Trip.VEHICLES_COLUMN_SEATED_CAPACITY)
                FastTripsLogger.warn("\n%s" % trips_df[[Trip.VEHICLES_COLUMN_VEHICLE_NAME, Trip.VEHICLES_COLUMN_SEATED_CAPACITY]].loc[pd.isnull(trips_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY])].drop_duplicates())

            # set standeeds
            trips_df[Trip.SIM_COL_VEH_STANDEES    ] = trips_df[Trip.SIM_COL_VEH_ONBOARD    ] - trips_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY]
            trips_df[Trip.SIM_COL_VEH_MSA_STANDEES] = trips_df[Trip.SIM_COL_VEH_MSA_ONBOARD] - trips_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY]
            # it can only be non-negative
            trips_df.loc[trips_df[Trip.SIM_COL_VEH_STANDEES    ]<0, Trip.SIM_COL_VEH_STANDEES    ] = 0
            trips_df.loc[trips_df[Trip.SIM_COL_VEH_MSA_STANDEES]<0, Trip.SIM_COL_VEH_MSA_STANDEES] = 0
            # where it is positive, friction = on+off+standees
            trips_df.loc[(trips_df[Trip.SIM_COL_VEH_STANDEES    ]>0)&(pd.notnull(trips_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY])), Trip.SIM_COL_VEH_FRICTION    ] = \
                trips_df[Trip.SIM_COL_VEH_BOARDS    ] + trips_df[Trip.SIM_COL_VEH_ALIGHTS    ] + trips_df[Trip.SIM_COL_VEH_STANDEES    ]
            trips_df.loc[(trips_df[Trip.SIM_COL_VEH_MSA_STANDEES]>0)&(pd.notnull(trips_df[Trip.VEHICLES_COLUMN_SEATED_CAPACITY])), Trip.SIM_COL_VEH_MSA_FRICTION] = \
                trips_df[Trip.SIM_COL_VEH_MSA_BOARDS] + trips_df[Trip.SIM_COL_VEH_MSA_ALIGHTS] + trips_df[Trip.SIM_COL_VEH_MSA_STANDEES]
        else:
            # log no seated capacities at all
            FastTripsLogger.warn("Trip.update_trip_times(): Cannot calculate friction because [%s] not configured" % Trip.VEHICLES_COLUMN_SEATED_CAPACITY)

        # Update the dwell time
        if Trip.VEHICLES_COLUMN_DWELL_FORMULA in trip_cols:
            all_dwell_df   = None
            all_dwell_init = False
            # grouppy unique dwell time forumulas
            dwell_groups = trips_df.groupby(Trip.VEHICLES_COLUMN_DWELL_FORMULA)
            for dwell_formula, dwell_group in dwell_groups:

                dwell_df = dwell_group.copy()
                FastTripsLogger.debug("dwell_formula %s has %d rows" % (str(dwell_formula), len(dwell_df)))
                if isinstance(dwell_formula,str):
                    if MSA_RESULTS:
                        # replace [boards], [alights], etc with trip_df['msa_boards'], trip_df['msa_alights'], etc
                        dwell_formula = dwell_formula.replace("[","dwell_df['msa_")
                    else:
                        # replace [boards], [alights], etc with trip_df['boards'], trip_df['alights'], etc
                        dwell_formula = dwell_formula.replace("[","dwell_df['")
                    dwell_formula = dwell_formula.replace("]","']")

                    # eval it
                    dwell_df[Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC] = eval(dwell_formula)
                else:
                    dwell_df[Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC] = 0.0

                # FastTripsLogger.debug("\n%s" % dwell_df[["boards","alights","dwell_formula","dwell_time_sec"]].to_string())
                if all_dwell_init:
                    all_dwell_df = pd.concat([all_dwell_df, dwell_df], axis=0)
                else:
                    all_dwell_df = dwell_df
                    all_dwell_init = True

            # use the new one
            trips_df = all_dwell_df

            # keep the dwell time
            trips_df[Trip.STOPTIMES_COLUMN_DWELL_TIME] = trips_df[Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC].map(lambda x: datetime.timedelta(seconds=x))

        # the vehicle stops if someone boards or someone alights or both
        trips_df["does_stop"] = (trips_df[Trip.SIM_COL_VEH_BOARDS]>0) | (trips_df[Trip.SIM_COL_VEH_ALIGHTS]>0)

        # we need information about the next stop
        next_stop_df = trips_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                 Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                 Trip.TRIPS_COLUMN_MAX_STOP_SEQUENCE,
                                 "does_stop"]].copy()
        next_stop_df.loc[:,"is_last_stop"] = next_stop_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE] == next_stop_df[Trip.TRIPS_COLUMN_MAX_STOP_SEQUENCE]
        next_stop_df.loc[:,Trip.STOPTIMES_COLUMN_STOP_SEQUENCE] = next_stop_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]-1
        next_stop_df.rename(columns={"does_stop"                       :"next_does_stop",
                                     "is_last_stop"                    :"next_is_last_stop"}, inplace=True)
        FastTripsLogger.debug("next_stop_df:\n%s\n" % next_stop_df.head().to_string())

        trips_df = pd.merge(left=trips_df, right=next_stop_df, how='left')
        assert(trips_df_len==len(trips_df))

        # Start with original travel time for the link FROM this stop to the NEXT stop
        trip_cols = list(trips_df.columns.values)

        # Add acceleration from stop if there are boards/alights
        # Skip first stop because we assume it's already there, and last since we don't go anywhere
        trips_df["accel_secs"] = 0.0
        if (Trip.VEHICLES_COLUMN_MAXIMUM_SPEED_FPS in trip_cols) and \
           (Trip.VEHICLES_COLUMN_ACCELERATION in trip_cols):
            trips_df.loc[trips_df["does_stop"] & \
                         (trips_df[Trip.VEHICLES_COLUMN_ACCELERATION] > 0) &
                         (trips_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]>1) & \
                         (trips_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE]<trips_df[Trip.TRIPS_COLUMN_MAX_STOP_SEQUENCE]), "accel_secs"] = \
                         trips_df[Trip.VEHICLES_COLUMN_MAXIMUM_SPEED_FPS]/trips_df[Trip.VEHICLES_COLUMN_ACCELERATION]
        # Add deceleration to next stop.
        # Skip stop with next stop = last stop because we assume it's already there
        trips_df["decel_secs"] = 0.0
        if (Trip.VEHICLES_COLUMN_MAXIMUM_SPEED_FPS in trip_cols) and \
           (Trip.VEHICLES_COLUMN_DECELERATION in trip_cols):
            trips_df.loc[(trips_df["next_does_stop"]) & \
                         (trips_df[Trip.VEHICLES_COLUMN_DECELERATION] > 0) &
                         (trips_df["next_is_last_stop"]==False), "decel_secs"] = \
                          trips_df[Trip.VEHICLES_COLUMN_MAXIMUM_SPEED_FPS]/trips_df[Trip.VEHICLES_COLUMN_DECELERATION]

        # update the travel time
        trips_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC] = trips_df[Trip.STOPTIMES_COLUMN_ORIGINAL_TRAVEL_TIME]/np.timedelta64(1, 's')
        trips_df.loc[ pd.notnull(trips_df["accel_secs"]), Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC] = trips_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC] + trips_df["accel_secs"]
        trips_df.loc[ pd.notnull(trips_df["decel_secs"]), Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC] = trips_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC] + trips_df["decel_secs"]

        trips_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME    ] = trips_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC].map(lambda x: datetime.timedelta(seconds=x))

        # put travel time + dwell together because that's the full time for a link (stop arrival time to next stop arrival time)
        trips_df["travel_dwell_sec"] = trips_df[Trip.STOPTIMES_COLUMN_TRAVEL_TIME_SEC] + trips_df[Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC]
        # cumulatively sum it to get arrival times times for the trip
        trips_df["travel_dwell_sec_cum"] = trips_df.groupby([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM])["travel_dwell_sec"].cumsum()
        trips_df["travel_dwell_cum"    ] = trips_df["travel_dwell_sec_cum"].map(lambda x: datetime.timedelta(seconds=x) if pd.notnull(x) else None)
        # verifying cumsum did as expected
        # FastTripsLogger.debug("\n"+ trips_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM, Trip.STOPTIMES_COLUMN_STOP_SEQUENCE, "travel_dwell_sec","travel_dwell_sec_cum"]].to_string())

        # move the next arrival time for joining to the next stop
        next_stop_df = trips_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                 Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                 Trip.TRIPS_COLUMN_TRIP_DEPARTURE_TIME,
                                 "travel_dwell_cum"]].copy()
        # need to start from trip arrival time.  For some reason can't aggregate STOPTIMES_COLUMN_DWELL_TIME, only the seconds version
        first_dwell_df = trips_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC]]. \
            groupby([Trip.STOPTIMES_COLUMN_TRIP_ID_NUM]).agg({Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC:'first'}).reset_index()
        first_dwell_df.rename(columns={Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC:"trip_first_dwell_sec"}, inplace=True)
        first_dwell_df["trip_first_dwell"] = first_dwell_df["trip_first_dwell_sec"].map(lambda x: datetime.timedelta(seconds=x))

        # verify first dwell is correct
        # FastTripsLogger.debug("first_dwell:\n%s\n" % first_dwell_df.head().to_string())
        next_stop_df = pd.merge(left=next_stop_df, right=first_dwell_df, how='left')

        FastTripsLogger.debug("next_stop_df:\n%s\n" % next_stop_df.head().to_string())

        next_stop_df["trip_arrival_time" ] = next_stop_df[Trip.TRIPS_COLUMN_TRIP_DEPARTURE_TIME] - next_stop_df["trip_first_dwell"]
        next_stop_df["new_arrival_time"  ] = next_stop_df["trip_arrival_time"] + trips_df["travel_dwell_cum"]
        next_stop_df.loc[:,Trip.STOPTIMES_COLUMN_STOP_SEQUENCE] += 1

        FastTripsLogger.debug("next_stop_df:\n%s\n" % next_stop_df.head().to_string())
        trips_df = pd.merge(left=trips_df,
                                right=next_stop_df[[Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
                                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                    "new_arrival_time"]],
                                how="left")
        # the first ones will be NaT but that's perfect -- we don't want to set those anyway
        trips_df.loc[pd.notnull(trips_df["new_arrival_time"]),Trip.STOPTIMES_COLUMN_ARRIVAL_TIME] = trips_df["new_arrival_time"]
        # set the first ones to be departure time minus dwell time
        trips_df.loc[pd.isnull( trips_df["new_arrival_time"]),Trip.STOPTIMES_COLUMN_ARRIVAL_TIME] = trips_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] - trips_df[Trip.STOPTIMES_COLUMN_DWELL_TIME]
        # departure time is arrival time + dwell
        trips_df.loc[:,Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] = trips_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME] + trips_df[Trip.STOPTIMES_COLUMN_DWELL_TIME]

        # float version
        trips_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME_MIN] = \
            trips_df[Trip.STOPTIMES_COLUMN_ARRIVAL_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + (x.time().second / 60.0) )
        trips_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME_MIN] = \
            trips_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME].map(lambda x: \
                60*x.time().hour + x.time().minute + (x.time().second / 60.0) )

        # FastTripsLogger.debug("Trips:update_trip_times() trips_df:\n%s\n" % \
        #     trips_df.loc[trips_df[Trip.TRIPS_COLUMN_MAX_STOP_SEQUENCE]>1,[Trip.STOPTIMES_COLUMN_TRIP_ID, Trip.STOPTIMES_COLUMN_TRIP_ID_NUM,
        #               Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
        #               Trip.STOPTIMES_COLUMN_ARRIVAL_TIME, Trip.STOPTIMES_COLUMN_DEPARTURE_TIME,
        #               Trip.VEHICLES_COLUMN_MAXIMUM_SPEED_FPS, Trip.VEHICLES_COLUMN_ACCELERATION, Trip.VEHICLES_COLUMN_DECELERATION,
        #               Trip. VEHICLES_COLUMN_SEATED_CAPACITY,
        #               Trip.SIM_COL_VEH_BOARDS, Trip.SIM_COL_VEH_ALIGHTS, Trip.SIM_COL_VEH_ONBOARD, Trip.SIM_COL_VEH_STANDEES, Trip.SIM_COL_VEH_FRICTION,
        #               "does_stop","next_does_stop","next_is_last_stop",
        #               "accel_secs","decel_secs",
        #               Trip.STOPTIMES_COLUMN_DWELL_TIME_SEC,
        #               "travel_dwell_sec","travel_dwell_sec_cum","new_arrival_time"
        #               ]].head(15).to_string())


        assert(trips_df_len==len(trips_df))
        # drop all the intermediate columns
        trips_df.drop(["does_stop","next_does_stop","next_is_last_stop",
                      "accel_secs","decel_secs",
                      "travel_dwell_sec","travel_dwell_sec_cum","travel_dwell_cum",
                      "new_arrival_time"], axis=1, inplace=True)
        FastTripsLogger.debug("trips_df.dtypes=\n%s\n" % str(trips_df.dtypes))

        return trips_df

    @staticmethod
    def linkify_vehicle_trips(veh_trips_df, stops):
        """
        Given a vehicle trips table with columns:

        Transforms and returns a link-version more relevant to passengers.
        The columns will be:
        * mode, mode_num, route_id, route_id_num, trip_id, trip_id_num
        * A_id, A_id_num, A_seq
        * B_id, B_id_num, B_seq
        * A_lat, A_lon
        * B_lat, B_lon
        * A_arrival_time, A_departure_time, B_arrival_time, B_departure_time
        """
        # FastTripsLogger.debug("linkify_vehicle_trips: veh_trips_df (%d)\n%s\n%s" % (len(veh_trips_df), veh_trips_df.head(), str(veh_trips_df.dtypes)))

        veh_temp_df = veh_trips_df[[Route.ROUTES_COLUMN_MODE,
                                    Trip.TRIPS_COLUMN_MODE_NUM,
                                    Trip.TRIPS_COLUMN_ROUTE_ID,
                                    Trip.TRIPS_COLUMN_ROUTE_ID_NUM,
                                    Trip.TRIPS_COLUMN_TRIP_ID,
                                    Trip.TRIPS_COLUMN_TRIP_ID_NUM,
                                    Trip.STOPTIMES_COLUMN_STOP_ID,
                                    "stop_id_num",
                                    Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                    Trip.STOPTIMES_COLUMN_ARRIVAL_TIME,
                                    Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]].copy()
        veh_temp_df = stops.add_stop_lat_lon(veh_temp_df, id_colname=Trip.STOPTIMES_COLUMN_STOP_ID, new_lat_colname="lat", new_lon_colname="lon")
        veh_temp_df["next_stop_seq"] = veh_temp_df[Trip.STOPTIMES_COLUMN_STOP_SEQUENCE] + 1

        # merge with next stop
        veh_temp_df = pd.merge(left     = veh_temp_df,
                                   left_on  = [Route.ROUTES_COLUMN_MODE, Trip.TRIPS_COLUMN_MODE_NUM,
                                               Trip.TRIPS_COLUMN_ROUTE_ID, Trip.TRIPS_COLUMN_ROUTE_ID_NUM,
                                               Trip.TRIPS_COLUMN_TRIP_ID,Trip.TRIPS_COLUMN_TRIP_ID_NUM, "next_stop_seq"],
                                   right    = veh_temp_df,
                                   right_on = [Route.ROUTES_COLUMN_MODE, Trip.TRIPS_COLUMN_MODE_NUM,
                                               Trip.TRIPS_COLUMN_ROUTE_ID, Trip.TRIPS_COLUMN_ROUTE_ID_NUM,
                                               Trip.TRIPS_COLUMN_TRIP_ID,Trip.TRIPS_COLUMN_TRIP_ID_NUM, Trip.STOPTIMES_COLUMN_STOP_SEQUENCE],
                                   how      = "left",
                                   suffixes = ["_A","_B"])
        # drop the ones that have NA - those are last links
        veh_temp_df = veh_temp_df.loc[ pd.notnull(veh_temp_df["stop_id_B"])]

        # rename -- maybe our convention should have put A and B as suffixes, eh?
        rename_cols = {"%s_A" % Trip.STOPTIMES_COLUMN_STOP_SEQUENCE:"A_seq",
                       "%s_B" % Trip.STOPTIMES_COLUMN_STOP_SEQUENCE:"B_seq",
                       "%s_A" % Trip.STOPTIMES_COLUMN_STOP_ID:"A_id",
                       "%s_B" % Trip.STOPTIMES_COLUMN_STOP_ID:"B_id",
                       "stop_id_num_A":"A_id_num",
                       "stop_id_num_B":"B_id_num"}
        for colname in ["lat", "lon",
                        Trip.STOPTIMES_COLUMN_ARRIVAL_TIME,
                        Trip.STOPTIMES_COLUMN_DEPARTURE_TIME]:
            rename_cols["%s_A" % colname] = "A_%s" % colname
            rename_cols["%s_B" % colname] = "B_%s" % colname
        veh_temp_df.rename(columns=rename_cols, inplace=True)

        # drop the next_stop_seq
        veh_temp_df.drop(["next_stop_seq_A", "next_stop_seq_B"], axis=1, inplace=True)

        # B_seq, B_id_num are ints.  they shouldn't be null
        veh_temp_df["B_seq"]    = veh_temp_df["B_seq"].astype(int)
        veh_temp_df["B_id_num"] = veh_temp_df["B_id_num"].astype(int)

        # FastTripsLogger.debug("linkify_vehicle_trips: veh_temp_df (%d)\n%s\n%s" % (len(veh_temp_df), veh_temp_df.head(30), str(veh_temp_df.dtypes)))

        return veh_temp_df

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

        stop_group_df = stop_group.apply(lambda x: x.sort_values(Trip.STOPTIMES_COLUMN_DEPARTURE_TIME))
        # set headway, in minutes
        stop_group_shift_df = stop_group_df.shift()
        stop_group_df['headway'] = (stop_group_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME] - stop_group_shift_df[Trip.STOPTIMES_COLUMN_DEPARTURE_TIME])/np.timedelta64(1,'m')
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
        trips_df = pd.merge(left  = trips_df,
                                right = stop_group_df[[Trip.STOPTIMES_COLUMN_TRIP_ID,
                                                       Trip.STOPTIMES_COLUMN_STOP_ID,
                                                       Trip.STOPTIMES_COLUMN_STOP_SEQUENCE,
                                                       'headway']],
                                on    = [Trip.STOPTIMES_COLUMN_TRIP_ID,
                                         Trip.STOPTIMES_COLUMN_STOP_ID,
                                         Trip.STOPTIMES_COLUMN_STOP_SEQUENCE])
        assert(len(trips_df)==trips_df_len)
        return trips_df
