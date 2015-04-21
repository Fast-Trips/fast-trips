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
import collections,datetime

from .Logger import FastTripsLogger

class Path:
    """
    Represents a path for a passenger from an origin :py:class:`TAZ` to a destination :py:class:`TAZ`
    through a set of stops.
    """

    DIR_OUTBOUND    = 1  #: Trips outbound from home have preferred arrival times
    DIR_INBOUND     = 2  #: Trips inbound to home have preferred departure times

    STATE_IDX_LABEL     = 0  #: :py:class:`datetime.timedelta` instance
    STATE_IDX_DEPARTURE = 1  #: :py:class:`datetime.datetime` instance
    STATE_IDX_DEPMODE   = 2  #: string or trip identifier
    STATE_IDX_SUCCESSOR = 3  #: stop identifier or TAZ identifier
    STATE_IDX_LINKTIME  = 4  #: :py:class:`datetime.timedelta` instance

    STATE_MODE_ACCESS   = "Access"
    STATE_MODE_EGRESS   = "Egress"
    STATE_MODE_TRANSFER = "Transfer"

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

    def path_str_header():
        """
        The header for the path file.
        """
        return "passengerId\tmode\toriginTaz\tdestinationTaz\tstartTime\tboardingStops\tboardingTrips\talightingStops\twalkingTimes"

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

        TODO: fix start time
        TODO: if the stop_ids and trip_ids are the correct type, no need to cast to int
        """
        boarding_stops  = ""
        boarding_trips  = ""
        alighting_stops = ""
        access_time     = 0
        transfer_time   = 0.0
        egress_time     = 0
        if len(self.states) > 1:
            for state_id,state in self.states.iteritems():
                FastTripsLogger.debug("%8s: %17s %s %10s %10s %10s" % (str(state_id), str(state[0]), state[1].strftime("%H:%M:%S"), 
                                      str(state[2]), str(state[3]), str(state[4])))

                # access
                if state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_ACCESS:
                    access_time     = state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0

                # transfer
                elif state[Path.STATE_IDX_DEPMODE] == Path.STATE_MODE_TRANSFER:
                    transfer_time  += state[Path.STATE_IDX_LINKTIME].total_seconds()/60.0

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

        return_str = "%s\t%s\t%s\t" % (str(self.mode), str(self.origin_taz_id), str(self.destination_taz_id))
        return_str += "%d\t%s\t%s\t%s\t" % (self.preferred_time.hour*60 + self.preferred_time.minute,
                                            boarding_stops,
                                            boarding_trips,
                                            alighting_stops)
        return_str += "%.2f,%.2f,%.2f" % (access_time, transfer_time, egress_time)
        return return_str