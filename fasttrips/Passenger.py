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
import os,sys
import pandas

from .Logger import FastTripsLogger
from .Path import Path

class Passenger:
    """
    Passenger class.  Documentation forthcoming.
    """

    #: File with demand
    #: TODO: document format
    INPUT_DEMAND_FILE = "ft_input_demand.dat"

    STATUS_INITIAL    = 0
    STATUS_WALKING    = 1
    STATUS_WAITING    = 2
    STATUS_ON_BOARD   = 3
    STATUS_BUMPED     = 4 #: bumped: couldn't board because vehicle is full
    STATUS_ARRIVED    = 5 #: arrived at destination

    def __init__(self, passenger_record):
        """
        Constructor from dictionary mapping attribute to value.
        """
        #: unique passenger identifier
        self.passenger_id       = passenger_record['passengerID']

        #: the remainder of the input is related to the :py:class:`Path`
        #: TODO: what about multiple trips for a single passenger?
        self.path               = Path(passenger_record)

        #: Simulation status.  One of :py:attr:`STATUS_INITIAL`, :py:attr:`STATUS_WALKING`,
        #: :py:attr:`STATUS_WAITING`, :py:attr:`STATUS_ON_BOARD`, :py:attr:`STATUS_BUMPED`,
        #: :py:attr:`STATUS_ARRIVED`
        self.simulation_status  = Passenger.STATUS_INITIAL

    def set_experienced_status_and_times(self, status, arrival_times, board_times, alight_times, destination_arrival):
        """
        Setter for :py:attr:`Passenger.simulation_status`.  Passes times through to :py:meth:`Path.set_experienced_times`
        """
        self.simulation_status = status
        self.path.set_experienced_times(arrival_times, board_times, alight_times, destination_arrival)

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

            if passenger.passenger_id == 100: break

        FastTripsLogger.info("Read %7d passengers" % len(passenger_id_to_passenger))
        return passenger_id_to_passenger