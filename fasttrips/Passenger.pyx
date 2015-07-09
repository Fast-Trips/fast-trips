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

    def __init__(self, passenger_record):
        """
        Constructor from dictionary mapping attribute to value.
        """
        #: passenger identifier, not necessarily unique
        self.passenger_id       = passenger_record['passengerID']

        #: the remainder of the input is related to the :py:class:`Path`
        #: TODO: what about multiple trips for a single passenger?
        self.path               = Path(passenger_record)

    @staticmethod
    def read_demand(input_dir):
        """
        Read the demand from the input file in *input_dir*.
        """
        demand_df = pandas.read_csv(os.path.join(input_dir, Passenger.INPUT_DEMAND_FILE), sep="\t")
        FastTripsLogger.debug("=========== DEMAND ===========\n" + str(demand_df.head()))
        FastTripsLogger.debug("\n"+str(demand_df.dtypes))

        passengers = []
        passenger_records = demand_df.to_dict(orient='records')
        for passenger_record in passenger_records:
            passengers.append(Passenger(passenger_record))

        FastTripsLogger.info("Read %7d passengers" % len(passengers))
        return passengers