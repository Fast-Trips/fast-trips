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
import collections,datetime,string

from .Logger import FastTripsLogger, DEBUG_NOISY

class Path:
    """
    Represents a path for a passenger from an origin :py:class:`TAZ` to a destination :py:class:`TAZ`
    through a set of stops.
    """
    #: Path configuration: Weight of in-vehicle time
    IN_VEHICLE_TIME_WEIGHT          = 1.0

    #: Path configuration: Weight of waiting time
    WAIT_TIME_WEIGHT                = 1.77

    #: Path configuration: Weight of access walk time
    WALK_ACCESS_TIME_WEIGHT         = 3.93

    #: Path configuration: Weight of egress walk time
    WALK_EGRESS_TIME_WEIGHT         = 3.93

    #: Path configuration: Weight of transfer walking time
    WALK_TRANSFER_TIME_WEIGHT       = 3.93

    #: Path configuration: Weight transfer penalty (minutes)
    TRANSFER_PENALTY                = 47.73

    #: Path configuration: Weight of schedule delay (0 - no penalty)
    SCHEDULE_DELAY_WEIGHT           = 0.0

    #: Path configuration: Fare in dollars per boarding (with no transfer credit)
    FARE_PER_BOARDING               = 0.0

    #: Path configuration: Value of time (dollars per hour)
    VALUE_OF_TIME                   = 999

    DIR_OUTBOUND    = 1  #: Trips outbound from home have preferred arrival times
    DIR_INBOUND     = 2  #: Trips inbound to home have preferred departure times

    STATE_IDX_LABEL     = 0  #: :py:class:`datetime.timedelta` instance
    STATE_IDX_DEPARTURE = 1  #: :py:class:`datetime.datetime` instance
    STATE_IDX_DEPMODE   = 2  #: string or trip identifier
    STATE_IDX_SUCCESSOR = 3  #: stop identifier or TAZ identifier
    STATE_IDX_LINKTIME  = 4  #: :py:class:`datetime.timedelta` instance
    STATE_IDX_COST      = 5  #: cost float, for hyperpath/stochastic assignment
    STATE_IDX_ARRIVAL   = 6  #: arrival time, a :py:class:`datetime.datetime` instance, for hyperpath/stochastic assignment

    STATE_MODE_ACCESS   = "Access"
    STATE_MODE_EGRESS   = "Egress"
    STATE_MODE_TRANSFER = "Transfer"

    BUMP_EXPERIENCED_COST = 999999

    def __init__(self, passenger_record):
        """
        Constructor from dictionary mapping attribute to value.
        """

        #: identifier for origin TAZ
        self.origin_taz_id      = passenger_record['OrigTAZ'    ]

        #: identifier for destination TAZ
        self.destination_taz_id = passenger_record['DestTAZ'    ]

        #: what is this?
        self.mode               = passenger_record['mode'       ]

        #: Demand time period (e.g. AM, PM, OP)
        self.time_period        = passenger_record['timePeriod' ]

        #: Should be one of :py:attr:`Path.DIR_OUTBOUND` or
        #: :py:attr:`Path.DIR_INBOUND`
        self.direction          = passenger_record['direction'  ]
        assert(self.direction in [Path.DIR_OUTBOUND, Path.DIR_INBOUND])

        #: Preferred arrival time if
        #: :py:attr:`Path.direction` == :py:attr:`Path.DIR_OUTBOUND` or
        #: preferred departure time if
        #: :py:attr:`Path.direction` == :py:attr:`Path.DIR_INBOUND`
        #: This is an instance of :py:class:`datetime.time`
        pref_time_int           = passenger_record['PAT'        ]
        self.preferred_time     = datetime.time(hour = int(pref_time_int/60.0),
                                                minute = pref_time_int % 60)

        #: This will include the stops and their related states
        #: Ordered dictionary: origin_taz_id -> state,
        #:                     stop_id -> state
        self.states = collections.OrderedDict()

        #: Experienced times arriving at a stop.  List of :py:class:`datetime.datetime` instances.
        self.experienced_arrival_times = None

        #: Experienced times boarding a vehicle.  List of :py:class:`datetime.datetime` instances.
        self.experienced_board_times   = None

        #: Experienced times alighting from vehicle.  List of :py:class:`datetime.datetime` instances.
        self.experienced_alight_times  = None

        #: Experienced arrival time at destination. A :py:class:`datetime.time` instance.
        self.experienced_destination_arrival = None

        #: Experienced cost in weighted minutes.
        self.experienced_cost = Path.BUMP_EXPERIENCED_COST

    def set_experienced_times(self, arrival_times, board_times, alight_times, destination_arrival):
        """
        Setter for :py:attr:`Path.experienced_arrival_times`, :py:attr:`Path.experienced_board_times`,
        and :py:attr:`Path.experienced_alight_times`.

        If passenger never arrives at destination, pass *destination_arrival* = *None*.

        Calculates and set :py:attr:`Path.experienced_cost`
        """
        self.experienced_arrival_times          = arrival_times
        self.experienced_board_times            = board_times
        self.experienced_alight_times           = alight_times
        self.experienced_destination_arrival    = destination_arrival

        if self.experienced_destination_arrival == None:
            return

        # Passenger arrived at their destination
        wait_time           = datetime.timedelta()
        walk_transfer_time  = datetime.timedelta()
        in_vehicle_time     = datetime.timedelta()
        for ride_num in range(len(self.experienced_board_times)):
            wait_time               += self.experienced_board_times[ride_num] - self.experienced_arrival_times[ride_num]
            if ride_num > 0:
                walk_transfer_time  += self.experienced_arrival_times[ride_num] - self.experienced_alight_times[ride_num-1]
            in_vehicle_time         += self.experienced_alight_times[ride_num] - self.experienced_board_times[ride_num]

        access_state = self.states.items()[0][1]
        egress_state = self.states.items()[-1][1]
        self.experienced_cost = \
            (Path.IN_VEHICLE_TIME_WEIGHT    * in_vehicle_time.total_seconds()/60.0                      ) + \
            (Path.WAIT_TIME_WEIGHT          * wait_time.total_seconds()/60.0                            ) + \
            (Path.WALK_TRANSFER_TIME_WEIGHT * walk_transfer_time.total_seconds()/60.0                   ) + \
            (Path.WALK_ACCESS_TIME_WEIGHT   * access_state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0) + \
            (Path.WALK_EGRESS_TIME_WEIGHT   * egress_state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0) + \
            (Path.TRANSFER_PENALTY          * (len(self.experienced_board_times)-1)                     ) + \
            (Path.FARE_PER_BOARDING         * len(self.experienced_board_times)*60.0/Path.VALUE_OF_TIME )

    def goes_somewhere(self):
        """
        Does this path go somewhere?  Does the destination differ from the origin?
        """
        return (self.origin_taz_id != self.destination_taz_id)

    def path_found(self):
        """
        Was a a transit path found from the origin to the destination with the constraints?
        """
        return len(self.states) > 1

    def experienced_arrival(self):
        """
        During simulation, did the passenger actually arrive?  (As opposed to getting bumped.)
        Returns boolean.
        """
        return (self.experienced_destination_arrival != None)

    def reset_states(self):
        """
        Delete my states, something went wrong and it won't work out.
        """
        self.states.clear()

    @staticmethod
    def state_str_header(state):
        """
        Returns a header for the state_str
        """
        if len(state) == 5:
            return "%8s: %17s  %9s %10s %10s %-17s" % \
            ("stop", "label", "departure", "dep_mode", "successor", "linktime")
        return "%8s: %12s %9s %10s %10s  %-17s %12s  %s" % \
            ("stop", "label", "departure", "dep_mode", "successor", "linktime", "cost", "arrival")

    @staticmethod
    def state_str(state_id, state):
        """
        Returns a readable string version of the given state_id and state, as a single line.
        """
        # deterministic
        if len(state) == 5:
            return "%8s: %-17s  %s %10s %10s %-17s" % \
            (str(state_id),
             str(state[Path.STATE_IDX_LABEL]),
             state[Path.STATE_IDX_DEPARTURE].strftime("%H:%M:%S"),
             str(state[Path.STATE_IDX_DEPMODE]),
             str(state[Path.STATE_IDX_SUCCESSOR]),
             str(state[Path.STATE_IDX_LINKTIME]))

        # stochastic
        return "%8s: %12.4f  %s %10s %10s  %-17s %12.4f  %s" % \
            (str(state_id),
             state[Path.STATE_IDX_LABEL],
             state[Path.STATE_IDX_DEPARTURE].strftime("%H:%M:%S"),
             str(state[Path.STATE_IDX_DEPMODE]),
             str(state[Path.STATE_IDX_SUCCESSOR]),
             str(state[Path.STATE_IDX_LINKTIME]),
             state[Path.STATE_IDX_COST],
             state[Path.STATE_IDX_ARRIVAL].strftime("%H.%M:%S"))

    def __str__(self):
        """
        Readable string version of the path.
        """
        if len(self.states) == 0: return "\nNo path"
        readable_str = "\n%s" % Path.state_str_header(self.states.items()[0][1])
        for state_id,state in self.states.iteritems():
            readable_str += "\n%s" % Path.state_str(state_id, state)
        return readable_str

    @staticmethod
    def path_str_header():
        """
        The header for the path file.
        """
        return "mode\toriginTaz\tdestinationTaz\tstartTime\tboardingStops\tboardingTrips\talightingStops\twalkingTimes"

    @staticmethod
    def time_str_header():
        """
        The header for the time file.
        """
        return "mode\toriginTaz\tdestinationTaz\tstartTime\tendTime\tarrivalTimes\tboardingTimes\talightingTimes\ttravelCost"

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

        TODO: if the stop_ids and trip_ids are the correct type, no need to cast to int
        """
        boarding_stops  = ""
        boarding_trips  = ""
        alighting_stops = ""
        start_time      = self.preferred_time
        access_time     = 0
        transfer_time   = ""
        egress_time     = 0
        prev_mode       = None
        if len(self.states) > 1:
            for state_id,state in self.states.iteritems():
                # access
                if state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_ACCESS:
                    access_time     = state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0
                    start_time      = state[Path.STATE_IDX_DEPARTURE]

                # transfer
                elif state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_TRANSFER:
                    transfer_time  += ",%.2f" % (state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0)

                # egress
                elif state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_EGRESS:
                    egress_time     = state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0

                # trip link
                else:
                    boarding_stops += "%ss%d" % ("," if len(boarding_stops) > 0 else "",
                                                 int(state_id))
                    boarding_trips += "%st%d" % ("," if len(boarding_trips) > 0 else "",
                                                 int(state[Path.STATE_IDX_DEPMODE]))
                    alighting_stops += "%ss%d" %("," if len(alighting_stops) > 0 else "",
                                                 int(state[Path.STATE_IDX_SUCCESSOR]))
                    # if the prev_mode is a trip link then we had a no-walk transfer
                    if prev_mode not in [Path.STATE_MODE_ACCESS, Path.STATE_MODE_TRANSFER, Path.STATE_MODE_EGRESS]:
                        transfer_time  += ",%.2f" % 0

                prev_mode = state[Path.STATE_IDX_DEPMODE]

        return_str = "%s\t%s\t%s\t" % (str(self.mode), str(self.origin_taz_id), str(self.destination_taz_id))
        return_str += "%.2f\t%s\t%s\t%s\t" % (start_time.hour*60.0 + start_time.minute + start_time.second/60.0,
                                            boarding_stops,
                                            boarding_trips,
                                            alighting_stops)
        # no transfers: access, trip egress
        return_str += "%.2f%s,%.2f" % (access_time, transfer_time, egress_time)
        return return_str

    def time_str(self):
        """
        String output of the experienced times, in legacy format (tab-delimited):

        * mode
        * origin TAZ
        * destination TAZ
        * start time, in minutes after midnight
        * end time (arrival at destination), in minutes after midnight
        * arrival times at stops, comma-delimited, in minutes after midnight
        * boarding times at stops, comma-delimited, in minutes after midnight
        * alighting times at stops, comma-delimited, in minutes after midnight
        * travel cost
        """
        start_time = self.preferred_time
        if len(self.states) > 1:
            start_time = self.states.items()[0][1][Path.STATE_IDX_DEPARTURE]

        return_str = "%s\t%s\t%s\t" % (str(self.mode), str(self.origin_taz_id), str(self.destination_taz_id))
        return_str += "%.2f\t" % (start_time.hour*60.0 + start_time.minute + start_time.second/60.0)
        return_str += "%.2f\t" % ((self.experienced_destination_arrival.hour*60 +
                                   self.experienced_destination_arrival.minute +
                                   self.experienced_destination_arrival.second/60.0) if self.experienced_destination_arrival else 0)
        return_str += string.join(["%.2f" % (x.hour*60.0 + x.minute + x.second/60.0) for x in self.experienced_arrival_times], ",")
        return_str += "\t"
        return_str += string.join(["%.2f" % (x.hour*60.0 + x.minute + x.second/60.0) for x in self.experienced_board_times], ",")
        return_str += "\t"
        return_str += string.join(["%.2f" % (x.hour*60.0 + x.minute + x.second/60.0) for x in self.experienced_alight_times], ",")
        return_str += "\t%.2f" % self.experienced_cost
        return return_str
