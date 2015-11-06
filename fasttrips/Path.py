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
import collections,datetime,os,string
import numpy,pandas

from .Logger    import FastTripsLogger
from .Passenger import Passenger
from .Util      import Util

#: Default user class: just one class called "all"
def generic_user_class(row_series):
    return "all"

class Path:
    """
    Represents a path for a passenger from an origin :py:class:`TAZ` to a destination :py:class:`TAZ`
    through a set of stops.
    """
    #: Paths output file
    PATHS_OUTPUT_FILE               = 'ft_output_passengerPaths.txt'

    #: Path times output file
    PATH_TIMES_OUTPUT_FILE          = 'ft_output_passengerTimes.txt'

    #: Configured functions, indexed by name
    CONFIGURED_FUNCTIONS            = { 'generic_user_class':generic_user_class }

    #: Path configuration: Name of the function that defines user class
    USER_CLASS_FUNCTION             = None

    #: Path weights.
    WEIGHTS                         = None

    #: todo: these will get removed in favor of WEIGHTS above
    IN_VEHICLE_TIME_WEIGHT          = 1.0
    WAIT_TIME_WEIGHT                = 1.77
    WALK_ACCESS_TIME_WEIGHT         = 3.93
    WALK_EGRESS_TIME_WEIGHT         = 3.93
    WALK_TRANSFER_TIME_WEIGHT       = 3.93
    TRANSFER_PENALTY                = 47.73
    SCHEDULE_DELAY_WEIGHT           = 0
    FARE_PER_BOARDING               = 0
    VALUE_OF_TIME                   = 999

    DIR_OUTBOUND    = 1  #: Trips outbound from home have preferred arrival times
    DIR_INBOUND     = 2  #: Trips inbound to home have preferred departure times

    STATE_IDX_LABEL         = 0  #: :py:class:`datetime.timedelta` instance
    STATE_IDX_DEPARR        = 1  #: :py:class:`datetime.datetime` instance. Departure if outbound/backwards, arrival if inbound/forwards.
    STATE_IDX_DEPARRMODE    = 2  #: mode id
    STATE_IDX_TRIP          = 3  #: trip id
    STATE_IDX_SUCCPRED      = 4  #: stop identifier or TAZ identifier
    STATE_IDX_SEQ           = 5  #: sequence (for trip)
    STATE_IDX_SEQ_SUCCPRED  = 6  #: sequence for successor/predecessor
    STATE_IDX_LINKTIME      = 7  #: :py:class:`datetime.timedelta` instance
    STATE_IDX_COST          = 8  #: cost float, for hyperpath/stochastic assignment
    STATE_IDX_ARRDEP        = 9  #: :py:class:`datetime.datetime` instance. Arrival if outbound/backwards, departure if inbound/forwards.

    STATE_MODE_ACCESS   = "Access"
    STATE_MODE_EGRESS   = "Egress"
    STATE_MODE_TRANSFER = "Transfer"

    # new
    STATE_MODE_TRIP     = "Trip" # onboard

    BUMP_EXPERIENCED_COST = 999999

    def __init__(self, trip_list_dict):
        """
        Constructor from dictionary mapping attribute to value.
        """
        self.__dict__.update(trip_list_dict)

        #: Direction is one of :py:attr:`Path.DIR_OUTBOUND` or :py:attr:`Path.DIR_INBOUND`
        #: Preferred time is a datetime.time object
        if trip_list_dict[Passenger.TRIP_LIST_COLUMN_TIME_TARGET] == "arrival":
            self.direction     = Path.DIR_OUTBOUND
            self.pref_time     = trip_list_dict[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME].to_datetime().time()
            self.pref_time_min = trip_list_dict[Passenger.TRIP_LIST_COLUMN_ARRIVAL_TIME_MIN]
        elif trip_list_dict[Passenger.TRIP_LIST_COLUMN_TIME_TARGET] == "departure":
            self.direction     = Path.DIR_INBOUND
            self.pref_time     = trip_list_dict[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME].to_datetime().time()
            self.pref_time_min = trip_list_dict[Passenger.TRIP_LIST_COLUMN_DEPARTURE_TIME_MIN]
        else:
            raise Exception("Don't understand trip_list %s: %s" % (Passenger.TRIP_LIST_COLUMN_TIME_TARGET, str(trip_list_dict)))

        #: This will include the stops and their related states
        #: Ordered dictionary: origin_taz_id -> state,
        #:                     stop_id -> state
        #: Note that if :py:attr:`Path.direction` is :py:attr:`Path.DIR_INBOUND`, then
        #: this is in reverse order (egress to access)
        self.states = collections.OrderedDict()

        #: Final path cost, will be filled in during path finding
        self.cost   = 0.0

    def goes_somewhere(self):
        """
        Does this path go somewhere?  Does the destination differ from the origin?
        """
        return (self.__dict__[Passenger.TRIP_LIST_COLUMN_ORIGIN_TAZ_ID] != self.__dict__[Passenger.TRIP_LIST_COLUMN_DESTINATION_TAZ_ID])

    def path_found(self):
        """
        Was a a transit path found from the origin to the destination with the constraints?
        """
        return len(self.states) > 1

    def reset_states(self):
        """
        Delete my states, something went wrong and it won't work out.
        """
        self.states.clear()

    def num_states(self):
        """
        Quick accessor for number of :py:attr:`Path.states`.
        """
        return len(self.states)

    def outbound(self):
        """
        Quick accessor to see if :py:attr:`Path.direction` is :py:attr:`Path.DIR_OUTBOUND`.
        """
        return self.direction == Path.DIR_OUTBOUND

    @staticmethod
    def set_user_class(trip_list_df, new_colname):
        """
        Adds a column called user_class by applying the configured user class function.
        """
        trip_list_df[new_colname] = trip_list_df.apply(Path.CONFIGURED_FUNCTIONS[Path.USER_CLASS_FUNCTION], axis=1)

    @staticmethod
    def state_str_header(state, direction=DIR_OUTBOUND):
        """
        Returns a header for the state_str
        """
        return "%8s: %-14s %9s %10s %11s %-17s %-12s  %s" % \
            ("stop",
             "label",
             "departure" if direction == Path.DIR_OUTBOUND else "arrival",
             "dep_mode"  if direction == Path.DIR_OUTBOUND else "arr_mode",
             "successor" if direction == Path.DIR_OUTBOUND else "predecessor",
             "linktime",
             "cost",
             "arrival"   if direction == Path.DIR_OUTBOUND else "departure")

    @staticmethod
    def state_str(state_id, state):
        """
        Returns a readable string version of the given state_id and state, as a single line.
        """
        return "%8s: %-14s  %s %10s %10s  %-17s %-12s  %s" % \
            (str(state_id),
             str(state[Path.STATE_IDX_LABEL]) if type(state[Path.STATE_IDX_LABEL])==datetime.timedelta else "%.4f" % state[Path.STATE_IDX_LABEL],
             state[Path.STATE_IDX_DEPARR].strftime("%H:%M:%S"),
             str(state[Path.STATE_IDX_DEPARRMODE]),
             str(state[Path.STATE_IDX_SUCCPRED]),
             str(state[Path.STATE_IDX_LINKTIME]),
             str(state[Path.STATE_IDX_COST]) if type(state[Path.STATE_IDX_COST])==datetime.timedelta else "%.4f" % state[Path.STATE_IDX_COST],
             state[Path.STATE_IDX_ARRDEP].strftime("%H.%M:%S"))

    def __str__(self):
        """
        Readable string version of the path.

        Note: If inbound trip, then the states are in reverse order (egress to access)
        """
        ret_str = "Dict vars:\n"
        for k,v in self.__dict__.iteritems():
            ret_str += "%30s => %-30s   %s\n" % (str(k), str(v), str(type(v)))
        ret_str += Path.states_to_str(self.states, self.direction)
        return ret_str

    @staticmethod
    def states_to_str(states, direction=DIR_OUTBOUND):
        """
        Given that states is an ordered dict of states, returns a string version of the path therein.
        """
        if len(states) == 0: return "\nNo path"
        readable_str = "\n%s" % Path.state_str_header(states.items()[0][1], direction)
        for state_id,state in states.iteritems():
            readable_str += "\n%s" % Path.state_str(state_id, state)
        return readable_str

    @staticmethod
    def write_paths(passengers_df, output_dir):
        """
        Write the assigned paths to the given output file.

        :param passengers_df: Passenger paths assignment results
        :type  passengers_df: :py:class:`pandas.DataFrame` instance
        :param output_dir:    Output directory
        :type  output_dir:    string

        """
        # get trip information -- board stops, board trips and alight stops
        passenger_trips = passengers_df.loc[passengers_df.linkmode==Path.STATE_MODE_TRIP].copy()
        ptrip_group     = passenger_trips.groupby(['person_id','trip_list_id_num'])
        # these are Series
        board_stops_str = ptrip_group.A_id.apply(lambda x:','.join(x))
        board_trips_str = ptrip_group.trip_id.apply(lambda x:','.join(x))
        alight_stops_str= ptrip_group.B_id.apply(lambda x:','.join(x))
        board_stops_str.name  = 'board_stop_str'
        board_trips_str.name  = 'board_trip_str'
        alight_stops_str.name = 'alight_stop_str'

        # get walking times
        walk_links = passengers_df.loc[(passengers_df.linkmode==Path.STATE_MODE_ACCESS  )| \
                                       (passengers_df.linkmode==Path.STATE_MODE_TRANSFER)| \
                                       (passengers_df.linkmode==Path.STATE_MODE_EGRESS  )].copy()
        walk_links['linktime_str'] = walk_links.linktime.apply(lambda x: "%.2f" % (x/numpy.timedelta64(1,'m')))
        walklink_group = walk_links[['person_id','trip_list_id_num','linktime_str']].groupby(['person_id','trip_list_id_num'])
        walktimes_str  = walklink_group.linktime_str.apply(lambda x:','.join(x))

        # aggregate to one line per person_id, trip_list_id
        print_passengers_df = passengers_df[['person_id','trip_list_id_num','pathmode','A_id','B_id','A_time']].groupby(['person_id','trip_list_id_num']).agg(
           {'pathmode'      :'first',   # path mode
            'A_id'          :'first',   # origin
            'B_id'          :'last',    # destination
            'A_time'        :'first'    # start time
           })

        # put them all together
        print_passengers_df = pandas.concat([print_passengers_df,
                                            board_stops_str,
                                            board_trips_str,
                                            alight_stops_str,
                                            walktimes_str], axis=1)

        print_passengers_df.reset_index(inplace=True)
        print_passengers_df.sort(columns=['trip_list_id_num'], inplace=True)

        print_passengers_df.rename(columns=
           {'pathmode'          :'mode',
            'A_id'              :'originTaz',
            'B_id'              :'destinationTaz',
            'A_time'            :'startTime_time',
            'board_stop_str'    :'boardingStops',
            'board_trip_str'    :'boardingTrips',
            'alight_stop_str'   :'alightingStops',
            'linktime_str'      :'walkingTimes'}, inplace=True)

        print_passengers_df['startTime'] = print_passengers_df['startTime_time'].apply(Util.datetime64_formatter)

        print_passengers_df = print_passengers_df[['trip_list_id_num','person_id','mode','originTaz','destinationTaz','startTime',
                                                   'boardingStops','boardingTrips','alightingStops','walkingTimes']]

        print_passengers_df.to_csv(os.path.join(output_dir, Path.PATHS_OUTPUT_FILE), sep="\t", index=False)
        # passengerId mode    originTaz   destinationTaz  startTime   boardingStops   boardingTrips   alightingStops  walkingTimes

    @staticmethod
    def write_path_times(pax_exp_df, output_dir):
        """
        Write the assigned path times to the given output file.

        :param pax_exp_df:   Passenger experienced paths (simulation results)
        :type  pax_exp_df:   :py:class:`pandas.DataFrame` instance
        :param output_dir:   Output directory
        :type  output_dir:   string
        """
        # reset columns
        print_pax_exp_df = pax_exp_df.reset_index()
        print_pax_exp_df.sort(columns=['trip_list_id_num'], inplace=True)

        print_pax_exp_df['A_time_str'] = print_pax_exp_df['A_time'].apply(Util.datetime64_formatter)
        print_pax_exp_df['B_time_str'] = print_pax_exp_df['B_time'].apply(Util.datetime64_formatter)

        # rename columns
        print_pax_exp_df.rename(columns=
            {'pathmode'             :'mode',
             'A_id'                 :'originTaz',
             'B_id'                 :'destinationTaz',
             'A_time_str'           :'startTime',
             'B_time_str'           :'endTime',
             'arrival_time_str'     :'arrivalTimes',
             'board_time_str'       :'boardingTimes',
             'alight_time_str'      :'alightingTimes',
             'cost'                 :'travelCost',
             }, inplace=True)

        # reorder
        print_pax_exp_df = print_pax_exp_df[[
            'trip_list_id_num',
            'person_id',
            'mode',
            'originTaz',
            'destinationTaz',
            'startTime',
            'endTime',
            'arrivalTimes',
            'boardingTimes',
            'alightingTimes',
            'travelCost']]

        times_out = open(os.path.join(output_dir, Path.PATH_TIMES_OUTPUT_FILE), 'w')
        print_pax_exp_df.to_csv(times_out,
                                sep="\t", float_format="%.2f", index=False)

    @staticmethod
    def path_str_header():
        """
        The header for the path file.
        """
        return "mode\toriginTaz\tdestinationTaz\tstartTime\tboardingStops\tboardingTrips\talightingStops\twalkingTimes"

    def path_str(self):
        """
        String output of the path, in legacy format (tab-delimited):

        * mode
        * origin TAZ
        * destination TAZ
        * start time, in minutes after midnight
        * boarding stop IDs, comma-delimited and prefixed with 's'
        * boarding trip IDs, comma-delimited and prefixed with 't'
        * alighting stop IDs, comma-delimited and prefixed with 's'
        * access time, sum of transfer times, egress time in number of minutes, comma-delimited

        .. todo:: if the stop_ids and trip_ids are the correct type, no need to cast to int
        """
        # OUTBOUND passengers have states like this:
        #    stop:          label    departure   dep_mode  successor linktime
        # orig_taz                                 Access    b stop1
        #  b stop1                                  trip1    a stop2
        #  a stop2                               Transfer    b stop3
        #  b stop3                                  trip2    a stop4
        #  a stop4                                 Egress   dest_taz
        #
        # e.g. (preferred arrival = 404 = 06:44:00)
        #    stop:          label    departure   dep_mode  successor linktime
        #      29: 0:24:29.200000     06:19:30     Access    23855.0 0:11:16.200000
        # 23855.0: 0:13:13            06:30:47   21649852    38145.0 0:06:51
        # 38145.0: 0:06:22            06:37:38   Transfer      38650 0:00:42
        #   38650: 0:05:40            06:38:20   25009729    76730.0 0:03:51.400000
        # 76730.0: 0:01:48.600000     06:42:11     Egress         18 0:01:48.600000
        #
        # INBOUND passengers have states like this
        #   stop:          label      arrival   arr_mode predecessor linktime
        # dest_taz                                 Egress    a stop4
        #  a stop4                                  trip2    b stop3
        #  b stop3                               Transfer    a stop2
        #  a stop2                                  trip1    b stop1
        #  b stop1                                 Access   orig_taz
        #
        # e.g. (preferred departure = 447 = 07:27:00)
        #    stop:          label      arrival   arr_mode predecessor linktime
        #    1586: 0:49:06            08:16:06     Egress    73054.0 0:06:27
        # 73054.0: 0:42:39            08:09:39   24201511    69021.0 0:13:11.600000
        # 69021.0: 0:29:27.400000     07:56:27   Transfer      68007 0:00:26.400000
        #   68007: 0:29:01            07:56:01   25539006    64065.0 0:28:11.200000
        # 64065.0: 0:00:49.800000     07:27:49     Access       3793 0:00:49.800000
        boarding_stops  = ""
        boarding_trips  = ""
        alighting_stops = ""
        start_time      = self.preferred_time
        access_time     = 0
        transfer_time   = ""
        egress_time     = 0
        prev_mode       = None
        if len(self.states) > 1:
            state_list = self.states.keys()
            if not self.outbound(): state_list = list(reversed(state_list))

            for state_id in state_list:
                state = self.states[state_id]
                # access
                if state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_ACCESS:
                    access_time     = state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0
                    start_time      = state[Path.STATE_IDX_DEPARR]
                    if not self.outbound(): start_time -= state[Path.STATE_IDX_LINKTIME]

                # transfer
                elif state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_TRANSFER:
                    transfer_time  += ",%.2f" % (state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0)

                # egress
                elif state[Path.STATE_IDX_DEPARRMODE] == Path.STATE_MODE_EGRESS:
                    egress_time     = state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0

                # trip link
                else:
                    if self.outbound():
                        boarding_stops += "%ss%d" % ("," if len(boarding_stops) > 0 else "",
                                                     int(state_id))
                        alighting_stops += "%ss%d" %("," if len(alighting_stops) > 0 else  "",
                                                     int(state[Path.STATE_IDX_SUCCPRED]))
                    else:
                        boarding_stops += "%ss%d" % ("," if len(boarding_stops) > 0 else "",
                                                     int(state[Path.STATE_IDX_SUCCPRED]))
                        alighting_stops += "%ss%d" %("," if len(alighting_stops) > 0 else  "",
                                                     int(state_id))

                    boarding_trips += "%st%d" % ("," if len(boarding_trips) > 0 else "",
                                                 int(state[Path.STATE_IDX_DEPARRMODE]))
                    # if the prev_mode is a trip link then we had a no-walk transfer
                    if prev_mode not in [Path.STATE_MODE_ACCESS, Path.STATE_MODE_TRANSFER, Path.STATE_MODE_EGRESS]:
                        transfer_time  += ",%.2f" % 0

                prev_mode = state[Path.STATE_IDX_DEPARRMODE]

        return_str = "%s\t%s\t%s\t" % (str(self.mode), str(self.origin_taz_id), str(self.destination_taz_id))
        return_str += "%.2f\t%s\t%s\t%s\t" % (start_time.hour*60.0 + start_time.minute + start_time.second/60.0,
                                            boarding_stops,
                                            boarding_trips,
                                            alighting_stops)
        # no transfers: access, trip egress
        return_str += "%.2f%s,%.2f" % (access_time, transfer_time, egress_time)
        return return_str
