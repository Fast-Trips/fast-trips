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
import datetime,os,sys
import pandas

from .Logger import FastTripsLogger

class Passenger:
    """
    Passenger class.  Documentation forthcoming.
    """

    #: File with demand
    #: TODO: document format
    INPUT_DEMAND_FILE = "ft_input_demand.dat"

    DIR_OUTBOUND    = 1  #: Trips outbound from home have preferred arrival times
    DIR_INBOUND     = 2  #: Trips inbound to home have preferred departure times

    def __init__(self, passenger_record):
        """
        Constructor from dictionary mapping attribute to value.
        """
        #: unique passenger identifier
        self.passenger_id       = passenger_record['passengerID']

        #: identifier for origin TAZ
        self.origin_taz_id      = passenger_record['OrigTAZ'    ]

        #: identifier for destination TAZ
        self.destination_taz_id = passenger_record['DestTAZ'    ]

        #: what is this?
        self.mode               = passenger_record['mode'       ]

        #: Demand time period (e.g. AM, PM, OP)
        self.time_period        = passenger_record['timePeriod' ]

        #: Should be one of :py:attr:`Passenger.DIR_OUTBOUND` or
        #: :py:attr:`Passenger.DIR_INBOUND`
        self.direction          = passenger_record['direction'  ]
        assert(self.direction in [Passenger.DIR_OUTBOUND, Passenger.DIR_INBOUND])

        #: Preferred arrival time if
        #: :py:attr:`Passenger.direction` == :py:attr:`Passenger.DIR_OUTBOUND` or
        #: preferred departure time if
        #: :py:attr:`Passenger.direction` == :py:attr:`Passenger.DIR_INBOUND`
        #: This is an instance of :py:class:`datetime.time`
        pref_time_int           = passenger_record['PAT'        ]
        self.preferred_time     = datetime.time(hour = int(pref_time_int/60.0),
                                                minute = pref_time_int % 60)

    @staticmethod
    def read_demand(input_dir):
        """
        Read the demand from the input file in *input_dir*.
        """
        demand_df = pandas.read_csv(os.path.join(input_dir, Passenger.INPUT_DEMAND_FILE), sep="\t")
        FastTripsLogger.debug("=========== DEMAND ===========\n" + str(demand_df.head()))
        FastTripsLogger.debug("\n"+str(demand_df.dtypes))

        passenger_id_to_passenger = {}
        passenger_records = demand_df.to_dict(orient='records')
        for passenger_record in passenger_records:
            passenger = Passenger(passenger_record)
            passenger_id_to_passenger[passenger.passenger_id] = passenger

        FastTripsLogger.info("Read %7d passengers" % len(passenger_id_to_passenger))
        return passenger_id_to_passenger