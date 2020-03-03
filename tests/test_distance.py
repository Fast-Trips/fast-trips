from __future__ import print_function
import os

import numpy as np
import pandas as pd
import partridge as ptg
import pytest

from fasttrips import Trip
from fasttrips import Util

TEST_NETWORKS = {"Seattle_Region": "psrc_1_1",
                 "Springfield"   : "vermont"}

EXAMPLES_DIR   = os.path.join(os.getcwd(), "fasttrips", "Examples")

@pytest.fixture(scope="module")
def network_results(network):
    results = {
        'Seattle_Region':
            {
                't1': [0.0000, 0.18204, 0.85835, 1.59093, 1.73259],
                't55': [0.0000, 1.40889],
                't140': [0.0000, 0.39525, 0.91519],
            },
        'Seattle_Region':
            {
                '690': [0.00000, 0.24679, 0.52990, 0.58124, 0.68396, 0.82198,
                        1.10185, 1.30837, 1.63678, 1.68605, 1.88833, 2.01921,
                        2.14929, 2.27598, 2.39962, 2.52896, 2.65403, 2.77906,
                        2.90012, 3.40607, 4.02007, 7.30269, 7.77643, 7.93774,
                        8.13528, 8.29669, 8.43537, 8.60926, 8.77880, 8.99127],
                '3942': [0.00000, 2.98571, 10.86012, 11.00405, 11.21411, 11.41179,
                         11.69441, 11.85530, 12.20669, 12.26657, 12.41157],
                '4023': [0.00000, 0.12492, 0.48199, 7.36683, 9.35049, 10.72752,
                         11.01201, 11.60369, 13.62171, 17.34048, 17.62048, 19.08759],
            }
    }
    yield results[network]

def test_calculate_distance_miles():
    orig_lat, orig_lon = 32.707431, -117.157058
    dest_lat, dest_lon = 32.740792, -117.211333
    cols = ['orig_lat','orig_lon','dest_lat','dest_lon','dist']

    df = pd.DataFrame([[orig_lat,orig_lon,dest_lat,dest_lon,np.nan]],
                      columns=cols)

    Util.calculate_distance_miles(df, cols[0], cols[1], cols[2], cols[3], cols[4])
    distance = df[cols[4]][0]

    print('test_calculate_distance_miles: {:.5f} mi'.format(distance))
    assert abs(distance - 3.9116) < 0.0001
