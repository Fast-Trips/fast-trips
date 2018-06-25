__copyright__ = "Copyright 2015-2016 Contributing Entities"
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

import os
import pandas as pd

def convert_stop_coordinates_to_mercator(row):
    from pyproj import Proj,transform
    """
       Expects to get a pandas data frame row from being called from
       an apply().  Outputs a pandas series in mercator coordinates
       appropriate for using with things like Bokeh map tiles.
    """
    inProj  = Proj(init='epsg:4326')
    outProj = Proj(init='epsg:3857')
    x2,y2 = transform(inProj,outProj,row['stop_lon'],row['stop_lat'])
    return pd.Series({'stop_m-lon':x2,'stop_m-lat':y2})

def convert_stop_coordinates_to_mercator(row):
    from pyproj import Proj,transform
    """
       Expects to get a pandas data frame row from being called from
       an apply().  Outputs a pandas series in mercator coordinates
       appropriate for using with things like Bokeh map tiles.
    """
    inProj  = Proj(init='epsg:4326')
    outProj = Proj(init='epsg:3857')
    x2,y2 = transform(inProj,outProj,row['stop_lon'],row['stop_lat'])
    return pd.Series({'stop_m-lon':x2,'stop_m-lat':y2})

def convert_taz_coords_to_mercator(row):
    from pyproj import Proj,transform
    """
       Expects to get a pandas data frame row from being called from
       an apply().  Outputs a pandas series in mercator coordinates
       appropriate for using with things like Bokeh map tiles.
    """
    inProj  = Proj(init='epsg:4326')
    outProj = Proj(init='epsg:3857')
    x2,y2 = transform(inProj,outProj,row['lon'],row['lat'])
    return pd.Series({'taz-lon':x2,'taz-lat':y2})

def prepare_demand(network_dir, demand_dir):
    """
    Input: 'trip_list.txt' from demand folder and 'taz_coors.txt' from networks folder.

    Output: Three dataframes:
     - `triplist_xy_df` trip list with added columns for: o-lat o-lon d-lat d-lon in Mercator
     - `trip_count_df`  aggregated trips to origins and destinations.  One row for each TAZ and columns for lat lon in Mercator
     - `od_trip_count_df`  aggregated trips to origin-destination pairs.

    """
    triplist_df = pd.read_csv(os.path.join(demand_dir,'trip_list.txt'), usecols=['departure_time', 'arrival_time', 'person_id','time_target','o_taz','d_taz' ])

    taz_df      = pd.read_csv(os.path.join(network_dir,'taz_coords.txt'))
    taz_xy_df   = taz_df.apply(convert_taz_coords_to_mercator, axis=1)
    taz_df      = pd.concat([taz_df,taz_xy_df], axis=1)
    taz_df.drop(columns=['lat','lon'],inplace=True)

    triplist_xy_df     = pd.merge(triplist_df, taz_df, left_on='o_taz', right_on='taz', how='left')
    triplist_xy_df.rename(index=str, columns={"taz-lon": "o-lon", "taz-lat": "o-lat"}, inplace=True)
    triplist_xy_df.drop(columns=['taz'],inplace=True)
    triplist_xy_df     = pd.merge(triplist_xy_df, taz_df,left_on='d_taz', right_on='taz',  how='left')
    triplist_xy_df.rename(index=str, columns={"taz-lon": "d-lon", "taz-lat": "d-lat"}, inplace=True)
    triplist_xy_df.drop(columns=['taz'],inplace=True)

    # GROUP ORIGIN TRIPS
    o_trip_count_df = triplist_xy_df.groupby(['o_taz']).size().reset_index(name='origins')
    o_trip_count_df.rename(columns={'o_taz':'taz'}, inplace=True)

    # GROUP DESTINATION TRIPS
    d_trip_count_df = triplist_xy_df.groupby(['d_taz']).size().reset_index(name='destinations')
    d_trip_count_df.rename(columns={'d_taz':'taz'}, inplace=True)

    # PUT TOGETHER
    trip_count_df   = pd.merge(taz_df, o_trip_count_df, how='left', on='taz')
    trip_count_df   = pd.merge(trip_count_df, d_trip_count_df, how='left', on='taz')

    # GROUP OD TRIPS
    od_trip_count_df = triplist_xy_df.groupby(['o_taz','d_taz']).size().reset_index(name='trips')

    od_trip_count_df   = pd.merge(od_trip_count_df, taz_df, left_on='o_taz', right_on='taz', how='left')
    od_trip_count_df.rename(index=str, columns={"taz-lon": "o-lon", "taz-lat": "o-lat"}, inplace=True)
    od_trip_count_df.drop(columns=['taz'],inplace=True)
    od_trip_count_df   = pd.merge(od_trip_count_df, taz_df,left_on='d_taz', right_on='taz',  how='left')
    od_trip_count_df.rename(index=str, columns={"taz-lon": "d-lon", "taz-lat": "d-lat"}, inplace=True)

    return triplist_xy_df, trip_count_df, od_trip_count_df, taz_df

def prepare_network_for_map(network_dir):
    stoptimes_df      = pd.read_csv(os.path.join(network_dir, 'stop_times.txt'), usecols=['trip_id','stop_id','stop_sequence'])
    stops_df          = pd.read_csv(os.path.join(network_dir, 'stops.txt'), usecols=['stop_id','stop_name','stop_lat','stop_lon'])
    xy_merc           = stops_df.apply(convert_stop_coordinates_to_mercator, axis=1)
    stops_df          = pd.concat([stops_df, xy_merc], axis=1)
    trip_stops_df     = pd.merge(stops_df, stoptimes_df, on='stop_id', how='left')
    trip_count_df     = trip_stops_df.groupby(['stop_id']).size().reset_index(name='trips')

    stops_size_df     = pd.merge(stops_df, trip_count_df, on='stop_id')
    return stops_size_df
