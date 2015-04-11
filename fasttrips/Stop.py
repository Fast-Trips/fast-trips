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

class Stop:
    """
    Stop class.  Documentation forthcoming.
    """

    #: File with stops.
    #: TODO document format
    INPUT_STOPS_FILE = "ft_input_stops.dat"

    #: File with transfers.
    #: TODO: document format
    INPUT_TRANSFERS_FILE = "ft_input_transfers.dat"

    def __init__(self, stop_record):
        """
        Constructor from dictionary mapping attribute to string value.

        {'stopName': 'AURORA AVE N & N 125TH ST',
         'capacity': 100L,
         'stopDescription': '_',
         'Longitude': -122.345047,
         'stopId': 7010L,
         'Latitude': 47.7197151}
        """
        self.stop_id            = stop_record['stopId'          ]
        self.name               = stop_record['stopName'        ]
        self.description        = stop_record['stopDescription' ]
        self.latitude           = stop_record['Latitude'        ]
        self.longitude          = stop_record['Longitude'       ]
        self.capacity           = stop_record['capacity'        ]

        #: destination stop_id -> (transfer_distance, transfer_time)
        self.transfers          = {}

        #: routes that I'm part of
        self.routes             = set()

    def add_transfer(self, transfer_record):
        """
        Add a transfer from this stop to the stop given in the transfer_record dictionary.
        """
        # self.stop_id == transfer_record['fromStop']
        self.transfers[transfer_record['toStop']] = (transfer_record['dist'], transfer_record['time'])

    def add_to_trip(self, trip):
        """
        Add myself to the given trip.
        """
        self.routes.add(trip.route_id)

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
