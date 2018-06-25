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

from map_prep import prepare_demand, prepare_network_for_map

from bokeh.models import ColumnDataSource, HoverTool, LabelSet
from bokeh.models.glyphs import MultiLine
from bokeh.plotting import figure, show, output_notebook
from bokeh.tile_providers import STAMEN_TONER
from bokeh.models import Arrow, OpenHead

def map_demand(demand, network):

    demand_xy_df, agg_demand_df, agg_od_df, taz_df = prepare_demand(network, demand)

    o_trip_count_df = demand_xy_df.groupby(['o_taz']).size().reset_index(name='o_trips')
    d_trip_count_df = demand_xy_df.groupby(['d_taz']).size().reset_index(name='d_trips')

    p_map = figure(x_axis_type="mercator", y_axis_type="mercator", title="Origin and Destinations for Transit Trips", width=800)
    p_map.grid.visible = False
    p_map.add_tile(STAMEN_TONER)

    source = ColumnDataSource(data=dict(
                             x    =list(agg_demand_df['taz-lon']),
                             y    =list(agg_demand_df['taz-lat']),
                             origins = list(agg_demand_df['origins']),
                             destinations = list(agg_demand_df['destinations'])))

    p_map.circle(x = 'x',
                 y = 'y',
                 source = source,
                 size = "origins",
                 legend = "origin",
                 fill_color="green",
                 line_color="green",
                 fill_alpha=0.15,
                 line_alpha=0.15)

    p_map.circle(x = 'x',
                 y = 'y',
                 source = source,
                 size = "destinations",
                 legend = "destination",
                 fill_color="navy",
                 line_color="blue",
                 line_width=4,
                 fill_alpha=0.01,
                 line_alpha=0.2)

    for index, row in agg_od_df.iterrows():
        x1 = row['o-lon']
        x2 = row['d-lon']
        y1 = row['o-lat']
        y2 = row['d-lat']

        p_map.line([x1,x2], [y1,y2], line_width= row['trips'], line_alpha=0.2)
        p_map.add_layout(Arrow(end=OpenHead(size=row['trips']), line_color="navy", line_alpha = 0.2,
            x_start=x1, y_start=y1, x_end=x2, y_end=y2))
    return p_map

def map_stops(network_dir, stop_labels):

    stops_size_df = prepare_network_for_map(network_dir)
    size_factor       = stops_size_df['trips'].max()/70
    if stops_size_df['trips'].max() < 10: size_factor = 0.05

    source = ColumnDataSource(data=dict(
                         x    =list(stops_size_df['stop_m-lon']),
                         y    =list(stops_size_df['stop_m-lat']),
                         trips=list(stops_size_df['trips']),
                         size =list(stops_size_df['trips']/size_factor),
                         offs =list(stops_size_df['trips']/(size_factor*2)),
                         name =list(stops_size_df['stop_name'])))

    hover = HoverTool(tooltips=[("stop", "@name"),("trips","@trips") ])

    p = figure(x_axis_type="mercator", y_axis_type="mercator", title = "Transit Stop Map", width = 800)

    p.add_tile(STAMEN_TONER)

    p.circle(x = 'x',
         y = 'y',
         source = source,
         size = 'size',
         fill_color="#FF1493",
         line_color="#FF69B4",
         fill_alpha=0.15,
         line_alpha=0.40)

    labels = LabelSet(x='x', y='y', text='name', level='glyph',text_color="#FF1493",
              x_offset='offs', y_offset='offs', source=source, render_mode='canvas')

    if stop_labels: p.add_layout(labels)

    return p
