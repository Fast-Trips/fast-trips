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

from map_prep import prepare_demand

from bokeh.models import ColumnDataSource, HoverTool, LabelSet
from bokeh.models.glyphs import MultiLine
from bokeh.plotting import figure, show, output_notebook
from bokeh.tile_providers import STAMEN_TONER

def plot_demand_time(demand_dir, aggregation_level=20):
    triplist_df = pd.read_csv(os.path.join(demand_dir,'trip_list.txt'), usecols=['departure_time', 'arrival_time', 'person_id','time_target' ])
    triplist_df["freq"] = 1
    triplist_df["departure_time_dt"] = pd.to_datetime(triplist_df["departure_time"])
    triplist_df["arrival_time_dt"]   = pd.to_datetime(triplist_df["arrival_time"])
    depart_agg = triplist_df[triplist_df['time_target']=='departure'].resample('%dT' % (aggregation_level), on='departure_time_dt').sum().rename(columns={'freq': 'trips_depart'})
    arrive_agg = triplist_df[triplist_df['time_target']=='arrival'].resample('%dT' % (aggregation_level), on='arrival_time_dt').sum().rename(columns={'freq': 'trips_arrive'})

    p = figure(plot_height=350, x_axis_type="datetime",toolbar_location=None, tools="", title="Time of Day Distribution of Transit Trips in Demand", width=800)
    p.xaxis.axis_label = "Time of Day"
    p.yaxis.axis_label = "Number of trips, aggregated to %d minutes" % (aggregation_level)
    p.line(x=list(depart_agg.index), y=list(depart_agg['trips_depart']), legend="departures", color="green", line_width=2)
    p.line(x=list(arrive_agg.index), y=list(arrive_agg['trips_arrive']), legend="arrivals", color="blue", line_width=2)

    p.vbar(x=[x for x in list(depart_agg.index)], width=aggregation_level/2, bottom=0, top=list(depart_agg['trips_depart']), color="green")
    p.vbar(x=[x for x in list(arrive_agg.index)], width=aggregation_level/2, bottom=0, top=list(arrive_agg['trips_arrive']), color="blue")
    return p

def plot_route_timing(network_dir, sel_routes=[]):
    stoptimes_df      = pd.read_csv(os.path.join(network_dir, 'stop_times.txt'), usecols=['trip_id','stop_sequence','arrival_time'])
    firststop_df      = stoptimes_df[stoptimes_df['stop_sequence']==1]
    trips_df          = pd.read_csv(os.path.join(network_dir, 'trips.txt'))
    firststop_df      = pd.merge(firststop_df, trips_df, on='trip_id', how='left')

    if 'direction_id' not in firststop_df:
        firststop_df['direction_id'] = 0

    firststop_df.loc[firststop_df['direction_id']==0,'route_id-dir'] = firststop_df.loc[firststop_df['direction_id']==0,'route_id']+"-Out"
    firststop_df.loc[firststop_df['direction_id']==1,'route_id-dir'] = firststop_df.loc[firststop_df['direction_id']==1,'route_id']+"-In"

    routes = sorted(list(set(firststop_df["route_id-dir"])))

    out_df = firststop_df[firststop_df['direction_id']==0]
    in_df  = firststop_df[firststop_df['direction_id']==1]

    route_in = ColumnDataSource(data=dict(
                   x    =[ pd.to_datetime(t) for t in list(in_df['arrival_time'])],
                   y    =list(in_df['route_id-dir']),
                   t    =list(in_df['arrival_time']),
                   name =list(in_df['trip_id'])))

    route_out = ColumnDataSource(data=dict(
                   x    =[ pd.to_datetime(t) for t in list(out_df['arrival_time'])],
                   y    =list(out_df['route_id-dir']),
                   t    =list(out_df['arrival_time']),
                   name =list(out_df['trip_id'])))

    tips=[
        ("trip", "@name"),
        ("time", "@t"),
    ]

    p_route = figure(x_axis_type="datetime",width=800, y_range=routes,
                     title='Route Timing')

    p_route.circle(x = 'x',
             y = 'y',
             source = route_in,
             size = 20,
             fill_color="#0892D0",
             line_color="#0892D0",
             fill_alpha=.5,
             line_alpha=0.40)

    p_route.circle(x = 'x',
             y = 'y',
             source = route_out,
             size = 20,
             fill_color="#FF1493",
             line_color="#FF69B4",
             fill_alpha=.5,
             line_alpha=0.40)

    p_route.add_tools(HoverTool(tooltips=tips))

    return p_route
