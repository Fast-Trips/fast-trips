import argparse, os

import pandas as pd

import fasttrips


USAGE = r"""

    Creates a tableau file with points for pathset paths.

    """

def add_taz_coords(network_dir, pathset_links_df):
    fasttrips.FastTripsLogger.info("Adding TAZ coordinates")
    # just need the taz coords
    taz_coords_file = os.path.join(network_dir, "taz_coords.txt")
    taz_coords_df = pd.read_csv(taz_coords_file, dtype={"taz":object})
    fasttrips.FastTripsLogger.debug("taz_coords_df=\n%s" % str(taz_coords_df.head()))

    # join to links
    pathset_links_df = pd.merge(left    =pathset_links_df,
                                    left_on ="A_id",
                                    right   =taz_coords_df,
                                    right_on="taz",
                                    how     ="left")
    # get lat, lon for access links
    pathset_links_df.loc[pathset_links_df["linkmode"]=="access", "A_lat"] = pathset_links_df["lat"]
    pathset_links_df.loc[pathset_links_df["linkmode"]=="access", "A_lon"] = pathset_links_df["lon"]
    pathset_links_df.drop(["taz","lat","lon"], axis=1, inplace=True)

    pathset_links_df = pd.merge(left    =pathset_links_df,
                                    left_on ="B_id",
                                    right   =taz_coords_df,
                                    right_on="taz",
                                    how     ="left")
    # get lat, lon for egress links
    pathset_links_df.loc[pathset_links_df["linkmode"]=="egress", "B_lat"] = pathset_links_df["lat"]
    pathset_links_df.loc[pathset_links_df["linkmode"]=="egress", "B_lon"] = pathset_links_df["lon"]
    pathset_links_df.drop(["taz","lat","lon"], axis=1, inplace=True)
    return pathset_links_df

def add_stop_coords(ft, pathset_links_df):
    fasttrips.FastTripsLogger.info("Adding stop coordinates")
    # add stop lats and lons if they're not there
    pathset_links_df = ft.stops.add_stop_lat_lon(pathset_links_df,
                                                 id_colname="A_id",
                                                 new_lat_colname="stop_A_lat",
                                                 new_lon_colname="stop_A_lon")
    pathset_links_df = ft.stops.add_stop_lat_lon(pathset_links_df,
                                                 id_colname="B_id",
                                                 new_lat_colname="stop_B_lat",
                                                 new_lon_colname="stop_B_lon")
    # not valid for access A_id and egress B_id
    pathset_links_df.loc[pathset_links_df["linkmode"]!="access", "A_lat"] = pathset_links_df["stop_A_lat"]
    pathset_links_df.loc[pathset_links_df["linkmode"]!="access", "A_lon"] = pathset_links_df["stop_A_lon"]
    pathset_links_df.loc[pathset_links_df["linkmode"]!="egress", "B_lat"] = pathset_links_df["stop_B_lat"]
    pathset_links_df.loc[pathset_links_df["linkmode"]!="egress", "B_lon"] = pathset_links_df["stop_B_lon"]
    pathset_links_df.drop(["stop_A_lat","stop_A_lon",
                           "stop_B_lat","stop_B_lon"], axis=1, inplace=True)
    return pathset_links_df

def impute_nostop_transit_link_stops(ft, pathset_links_df, veh_trips_df, prev_next):
    """
    Tries to fill in A_id, A_seq, A_lat, A_lon, B_id, B_seq, B_lat, B_lon, for transit links that have none of those filled in.
    """
    fasttrips.FastTripsLogger.info("Imputing nostop transit link stops using %s link" % prev_next)
    fasttrips.FastTripsLogger.debug("impute_nostop_transit_link_stops: veh_trips_df head=\n%s" % str(veh_trips_df.head()))

    prevnext_links_df = pathset_links_df[["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum","linkmode",
                                          "A_id","A_id_num","A_lat","A_lon",
                                          "B_id","B_id_num","B_lat","B_lon"]].copy()
    if prev_next=="prev":
        prevnext_links_df["linknum"] = prevnext_links_df["linknum"]+1
    elif prev_next=="next":
        prevnext_links_df["linknum"] = prevnext_links_df["linknum"]-1
    else: # this shouldn't happen
        raise

    # get next or prev link information on the link
    pathset_links_df = pd.merge(left    =pathset_links_df,
                                    right   =prevnext_links_df,
                                    how     ="left",
                                    on      =["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum"],
                                    suffixes=["","_%s" % prev_next])

    # target links to imput nodes
    find_near_df = None
    AB           = None
    if prev_next=="prev":
        AB = "A"  # this is what we're setting
        # A_id: if the previous link is access and the B_id is null, get the A_id (taz) and find a stop_id close to it
        # A_id: if the previous link is transfer and the B_id is null, get the A_id and find a stop_id close to it
        find_near_df = pathset_links_df.loc[ ((pathset_links_df["linkmode_prev"]=="access")|(pathset_links_df["linkmode_prev"]=="transfer"))&
                                              pd.isnull(pathset_links_df["A_id"]     )&
                                              pd.isnull(pathset_links_df["B_id_prev"])&
                                              pd.notnull(pathset_links_df["A_id_prev"]) ]
        fasttrips.FastTripsLogger.info("Imputing A_id from previous link for %d links" % len(find_near_df))

    else:
        AB = "B"  # this is what we're setting
        # B_id: if the next link is transfer and A_id is null, get the B_id and find a stop_id close to it
        # B_id: if the next link is egress and the A_id is null, get the B_id (taz) and find a stop_id close to it
        find_near_df = pathset_links_df.loc[ ((pathset_links_df["linkmode_next"]=="egress")|(pathset_links_df["linkmode_next"]=="transfer"))&
                                              pd.isnull(pathset_links_df["B_id"]     )&
                                              pd.isnull(pathset_links_df["A_id_next"])&
                                              pd.notnull(pathset_links_df["B_id_next"]) ]
        fasttrips.FastTripsLogger.info("Imputing B_id from next link for %d links" % len(find_near_df))

    # nothing to do
    if len(find_near_df) == 0: return pathset_links_df

    fasttrips.FastTripsLogger.debug("find_near_df=\n%s" % find_near_df.head(30))

    # do it by service id
    service_id_list = find_near_df["service_id"].value_counts().keys()
    impute_count = 0
    for service_id in service_id_list:

        impute_id = find_near_df.loc[ find_near_df["service_id"]==service_id]
        fasttrips.FastTripsLogger.debug("Processing service_id [%s] with %d possible imputes" % (service_id, len(impute_id)))

        service_vehicle_stops = veh_trips_df.loc[ veh_trips_df["service_id"] == service_id, ["service_id","mode","stop_id","stop_id_num","stop_name","stop_lat","stop_lon"]].drop_duplicates()

        fasttrips.FastTripsLogger.debug("impute_id len=%d head()=\n%s" % (len(impute_id), str(impute_id.head())))
        fasttrips.FastTripsLogger.debug("service_vehicle_stops len=%d head()=\n%s" % (len(service_vehicle_stops), str(service_vehicle_stops.head())))

        near_stops_df = pd.merge(left =impute_id,
                                     right=service_vehicle_stops,
                                     on   =["service_id","mode"],
                                     how  ="left")
        # no join success -- nothing we can do
        if pd.notnull(near_stops_df["stop_id"]).sum() == 0:
            fasttrips.FastTripsLogger.info("Imputing %6d out of %6d stops for %s" % (0, len(impute_id), service_id))

        else:
            # calculate the distance from [person_prefix]_lat, [person_prefix]_lon, and the stop
            fasttrips.Util.calculate_distance_miles(near_stops_df,
                                                    origin_lat      ="%s_lat_%s" % (AB, prev_next),
                                                    origin_lon      ="%s_lon_%s" % (AB, prev_next),
                                                    destination_lat ="stop_lat",
                                                    destination_lon ="stop_lon",
                                                    distance_colname="stop_dist")
            fasttrips.FastTripsLogger.debug("near_stops_df len=%d head()=\n%s" % (len(near_stops_df), str(near_stops_df.head())))

            # pick the closest one
            near_stops_df = near_stops_df.loc[ near_stops_df.groupby(["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum"])["stop_dist"].idxmin(),
              ["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum","stop_id","stop_id_num","stop_name","stop_lat","stop_lon","stop_dist"] ]

            fasttrips.FastTripsLogger.debug("near_stops_df len=%d head()=\n%s" % (len(near_stops_df), str(near_stops_df.head())))

            # set it into pathset_links_df
            pathset_links_df = pd.merge(left=pathset_links_df,
                                            right=near_stops_df,
                                            on=["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum"],
                                            how="left",
                                            suffixes=["","_near"])
            fasttrips.FastTripsLogger.debug("pathset_links_df head=\n%s" % str(pathset_links_df.loc[ pd.notnull(pathset_links_df["stop_id"])].head()))
            # set it
            fasttrips.FastTripsLogger.info("Imputing %6d out of %6d stops for %s" % (pd.notnull(pathset_links_df["stop_id"]).sum(), len(impute_id), service_id))
            impute_count += pd.notnull(pathset_links_df["stop_id"]).sum()
            pathset_links_df.loc[ pd.notnull(pathset_links_df["stop_id"]), "%s_id"     % AB] = pathset_links_df["stop_id"    ]
            pathset_links_df.loc[ pd.notnull(pathset_links_df["stop_id"]), "%s_id_num" % AB] = pathset_links_df["stop_id_num"]
            pathset_links_df.loc[ pd.notnull(pathset_links_df["stop_id"]), "%s_lat"    % AB] = pathset_links_df["stop_lat"   ]
            pathset_links_df.loc[ pd.notnull(pathset_links_df["stop_id"]), "%s_lon"    % AB] = pathset_links_df["stop_lon"   ]
            pathset_links_df.loc[ pd.notnull(pathset_links_df["stop_id"]), "%s_impute" % AB] = True

            # we're done with these fields
            pathset_links_df.drop(["stop_id","stop_id_num","stop_name","stop_lat","stop_lon","stop_dist"], axis=1, inplace=True)

    fasttrips.FastTripsLogger.info("=> Imputed  %6d total" % impute_count)
    # drop the prev columns
    pathset_links_df.drop(["linkmode_%s" % prev_next,
                           "A_id_%s" % prev_next,"A_id_num_%s" % prev_next,"A_lat_%s" % prev_next,"A_lon_%s" % prev_next,
                           "B_id_%s" % prev_next,"B_id_num_%s" % prev_next,"B_lat_%s" % prev_next,"B_lon_%s" % prev_next], axis=1, inplace=True)
    return pathset_links_df

def impute_stop_from_adjacent_stop(ft, pathset_links_df, prev_next):
    """
    If prev_next is "next", sets B from next A.
    If prev_next is "prev", sets A from prev B.
    """
    AB_set = None  # setting this
    AB_use = None  # using this
    if prev_next=="next":
        AB_set = "B"
        AB_use = "A"
    elif prev_next=="prev":
        AB_set = "A"
        AB_use = "B"
    else:
        raise

    target_count = pd.isnull(pathset_links_df["%s_id" % AB_set]).sum()
    if target_count == 0: return pathset_links_df

    # fill in unknown Bs from next link
    fasttrips.FastTripsLogger.info("Trying to impute %8d null %s_id values" % (target_count, AB_set))
    prevnext_links_df = pathset_links_df[["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum",
                                          "%s_id" % AB_use,"%s_id_num" % AB_use, "%s_lat" % AB_use, "%s_lon" % AB_use]].copy()
    if prev_next=="next":
        prevnext_links_df["linknum"] = prevnext_links_df["linknum"]-1
    else:
        prevnext_links_df["linknum"] = prevnext_links_df["linknum"]+1

    pathset_links_df = pd.merge(left    =pathset_links_df,
                                    right   =prevnext_links_df,
                                    how     ="left",
                                    on      =["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum"],
                                    suffixes=["","_%s" % prev_next])

    # inpute the AB_set
    impute_count = len(pathset_links_df.loc[ pd.isnull(pathset_links_df["%s_id" % AB_set])&pd.notnull(pathset_links_df["%s_id_%s" % (AB_use, prev_next)])])
    pathset_links_df.loc[ pd.isnull(pathset_links_df["%s_id" % AB_set])&pd.notnull(pathset_links_df["%s_id_%s" % (AB_use, prev_next)]), "%s_impute" % AB_set] = True
    pathset_links_df.loc[ pd.isnull(pathset_links_df["%s_id" % AB_set])&pd.notnull(pathset_links_df["%s_id_%s" % (AB_use, prev_next)]), "%s_id_num" % AB_set] = pathset_links_df["%s_id_num_%s" % (AB_use, prev_next)]
    pathset_links_df.loc[ pd.isnull(pathset_links_df["%s_id" % AB_set])&pd.notnull(pathset_links_df["%s_id_%s" % (AB_use, prev_next)]), "%s_lat"    % AB_set] = pathset_links_df["%s_lat_%s"    % (AB_use, prev_next)]
    pathset_links_df.loc[ pd.isnull(pathset_links_df["%s_id" % AB_set])&pd.notnull(pathset_links_df["%s_id_%s" % (AB_use, prev_next)]), "%s_lon"    % AB_set] = pathset_links_df["%s_lon_%s"    % (AB_use, prev_next)]
    pathset_links_df.loc[ pd.isnull(pathset_links_df["%s_id" % AB_set])&pd.notnull(pathset_links_df["%s_id_%s" % (AB_use, prev_next)]), "%s_id"     % AB_set] = pathset_links_df["%s_id_%s"     % (AB_use, prev_next)]
    # done
    pathset_links_df.drop(["%s_id_%s"     % (AB_use, prev_next),
                           "%s_id_num_%s" % (AB_use, prev_next),
                           "%s_lat_%s"    % (AB_use, prev_next),
                           "%s_lon_%s"    % (AB_use, prev_next)], axis=1, inplace=True)

    fasttrips.FastTripsLogger.info("Imputed %8d values for %s_id => Have %8d null %s_id values" % (impute_count, AB_set, pd.isnull(pathset_links_df["%s_id" % AB_set]).sum(), AB_set))
    return pathset_links_df

if __name__ == "__main__":

    pd.set_option('display.width',      1000)
    pd.set_option('display.max_rows',   1000)
    pd.set_option('display.max_columns', 100)

    parser = argparse.ArgumentParser(description=USAGE)
    parser.add_argument('input_network_dir', type=str, nargs=1, help="Directory with input network")
    parser.add_argument('input_path_dir',    type=str, nargs=1, help="Directory with pathset_[links,paths].csv files")
    parser.add_argument('--description', dest='use_description', action='store_true', 
        help="Specify this to use path description as ID.  Otherwise will use standard pathset fields (person_id, person_trip_id, iteration, pathfinding_iteration, simulation_iteration, pathnum)")

    args = parser.parse_args()
    LOG_DIR = "create_tableau_path_map"

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    # Use fasttrips to read the network
    ft = fasttrips.FastTrips(input_network_dir= args.input_network_dir[0],
                             input_demand_dir = None,
                             output_dir       = LOG_DIR)
    ft.read_input_files()

    # and the pathset files
    (pathset_paths_df, pathset_links_df) = ft.passengers.read_passenger_pathsets(args.input_path_dir[0], ft.stops, ft.routes.modes_df)
    fasttrips.Assignment.TRACE_PERSON_IDS = pathset_paths_df["person_id"].head(5).tolist()
    fasttrips.FastTripsLogger.debug("Read passenger pathsets. head=\n%s" % str(pathset_links_df.head(100)))

    pathset_links_df = add_taz_coords(args.input_network_dir[0], pathset_links_df)
    pathset_links_df = add_stop_coords(ft, pathset_links_df)

    # single path per person trip
    if "pathnum" not in list(pathset_paths_df.columns.values):
        pathset_paths_df["pathnum"] = 0
        pathset_links_df["pathnum"] = 0
    # add description if not there.  todo: handle its absence better instead?
    if "description" not in list(pathset_paths_df.columns.values):
        pathset_paths_df["description"] = "desc TBD"
    # add iteration if not there.  todo: handle its absence better instead?
    if "iteration" not in list(pathset_paths_df.columns.values):
        pathset_paths_df["iteration"] = 1
        pathset_links_df["iteration"] = 1
    if "pathfinding_iteration" not in list(pathset_paths_df.columns.values):
        pathset_paths_df["pathfinding_iteration"] = 1
        pathset_links_df["pathfinding_iteration"] = 1
    # add simulation_iteration if not there.  todo: handle its absence better instead?
    if "simulation_iteration" not in list(pathset_paths_df.columns.values):
        pathset_paths_df["simulation_iteration"] = 1
        pathset_links_df["simulation_iteration"] = 1

    # Fill in null A_id and B_id if we can
    pathset_links_df["A_impute"] = False
    pathset_links_df["B_impute"] = False

    # first, try to impute transit links for which nothing is known
    veh_trips_df = ft.trips.get_full_trips()
    veh_trips_df = ft.stops.add_stop_lat_lon(veh_trips_df, id_colname="stop_id", new_lat_colname="stop_lat", new_lon_colname="stop_lon", new_stop_name_colname="stop_name")

    pathset_links_df = impute_nostop_transit_link_stops(ft, pathset_links_df, veh_trips_df, "prev")
    pathset_links_df = impute_nostop_transit_link_stops(ft, pathset_links_df, veh_trips_df, "next")

    pathset_links_df = impute_stop_from_adjacent_stop(ft, pathset_links_df, "prev")
    pathset_links_df = impute_stop_from_adjacent_stop(ft, pathset_links_df, "next")

    # split the pathset links into component bits -- only the ones with trip_ids
    pathset_links_trip   = pathset_links_df.loc[pd.notnull(pathset_links_df["trip_id"])].copy()
    pathset_links_notrip = pathset_links_df.loc[pd.isnull(pathset_links_df["trip_id"])]
    fasttrips.FastTripsLogger.info("Splitting pathset_links_df (%d) into links with trip_id (%d) and links without (%d)" %
                                   (len(pathset_links_df), len(pathset_links_trip), len(pathset_links_notrip)))

    if len(pathset_links_trip) == 0:
        pathset_links_df = pathset_links_notrip
    else:
        pathset_links_trip   = fasttrips.PathSet.split_transit_links(pathset_links_trip, veh_trips_df, ft.stops)
        fasttrips.FastTripsLogger.info("Split links with trip_id into parts => %d links" % len(pathset_links_trip))
        pathset_links_df     = pathset_links_trip.append(pathset_links_notrip, ignore_index=True)
        fasttrips.FastTripsLogger.info("Back together to make %d pathset_links_df" % len(pathset_links_df))

    pathset_links_df.sort_values(by=["person_id","person_trip_id","pathnum","linknum"], inplace=True)
    pathset_links_df.reset_index(drop=True, inplace=True)

    fasttrips.FastTripsLogger.debug("pathset_links_df.head(100)=\n%s" % str(pathset_links_df.head(100)))

    missing_lat_lon = pathset_links_df.loc[pd.isnull(pathset_links_df["A_lat"])|pd.isnull(pathset_links_df["B_lat"])]
    if len(missing_lat_lon) > 0:
        fasttrips.FastTripsLogger.info("Missing %d lat/lons" % len(missing_lat_lon))

    # save this for debugging
    debug_file = "create_tableau_path_map_debug.csv"
    pathset_links_df.to_csv(debug_file, sep=",", index=False)
    fasttrips.FastTripsLogger.debug("Wrote %s" % debug_file)

    # select out just the fields we want for the map
    map_link_fields = ["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum",
                       "linknum","linkmode","mode","route_id","trip_id",
                       "A_id","A_seq","A_lat","A_lon",
                       "B_id","B_seq","B_lat","B_lon"]
    pathset_links_df = pathset_links_df[map_link_fields]

    # drop the B fields
    map_points_df = pathset_links_df[map_link_fields[:-4]].copy()

    pathset_paths_df = pathset_paths_df[["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum",
                                         "description"]]
    pathset_paths_df[["pathnum"]] = pathset_paths_df[["pathnum"]].astype(int)

    # Each link will be it's own map line.  A split link is a single map line but with a bunch of points.
    # Access, egresss and transit map lines will just be two points: A and B
    # So the line index is "person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum"

    # add point id starting with 1 for A; the B point IS is just that plus one
    map_points_df["point_id"] = map_points_df.groupby(["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum"]).cumcount() + 1
    # rename A_id, A_seq, A_lat, A_lon
    map_points_df.rename(columns={"A_id" :"stop_or_taz_id",
                                  "A_seq":"stop_sequence",
                                  "A_lat":"latitude",
                                  "A_lon":"longitude"}, inplace=True)

    fasttrips.FastTripsLogger.debug("map_points_df head=\n%s\ntail=\n%s" % (str(map_points_df.head(30)), str(map_points_df.tail(30))))

    # group for the last one
    pathset_links_df["point_id"]=1  # we need this set to *something* agg
    last_point_df = pathset_links_df.groupby(["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum"]).agg(
        {"point_id":"count",
         "B_id":"last",
         "B_seq":"last",
         "B_lat":"last",
         "B_lon":"last",
         "route_id":"last",
         "trip_id" :"last",
         "linkmode":"last",
         "mode"    :"last"}).reset_index()
    last_point_df.rename(columns={"B_id"   :"stop_or_taz_id",
                                  "B_seq"  :"stop_sequence",
                                  "B_lat"  :"latitude",
                                  "B_lon"  :"longitude"}, inplace=True)
    last_point_df["point_id"] = last_point_df["point_id"] + 1
    fasttrips.FastTripsLogger.debug("last_point_df=\n%s" % str(last_point_df.head(30)))

    # combine map points with the last map point
    map_points_df = map_points_df.append(last_point_df, ignore_index=True)
    map_points_df.sort_values(by=["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","linknum","point_id"], inplace=True)
    map_points_df.reset_index(drop=True, inplace=True)

    map_points_df[["pathnum","linknum"]] = map_points_df[["pathnum","linknum"]].astype(int)
    fasttrips.FastTripsLogger.debug("map_points_df head=\n%s\ntail=\n%s" % (str(map_points_df.head(100)), str(map_points_df.tail(100))))
    fasttrips.FastTripsLogger.info("Have %d map points" % len(map_points_df))

    if args.use_description:
        fasttrips.FastTripsLogger.info("Using description as ID")
        # get unique descriptions
        num_paths = len(pathset_paths_df)
        pathset_paths_df.drop_duplicates(subset=["description"], keep="first", inplace=True)
        fasttrips.FastTripsLogger.info("Dropping duplicate path descriptions.  Went from %d to %d paths" % (num_paths, len(pathset_paths_df)))

        # join map points to them
        map_points_df = pd.merge(left    =pathset_paths_df,
                                     left_on =["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum"],
                                     right   =map_points_df,
                                     right_on=["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum"],
                                     how     ="inner")
        fasttrips.FastTripsLogger.info("Have %d map points" % len(map_points_df))

        # drop the non-descript columns
        map_points_df.drop(["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum"], axis=1, inplace=True)
    else:
        # just add descriptions
        map_points_df = pd.merge(left    =map_points_df,
                                     left_on =["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum"],
                                     right   =pathset_paths_df[["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum","description"]],
                                     right_on=["person_id","person_trip_id","iteration","pathfinding_iteration","simulation_iteration","pathnum"],
                                     how     ="left")
    # write it
    output_file = os.path.join(args.input_path_dir[0], "pathset_map_points.csv")
    map_points_df.to_csv(output_file, sep=",", index=False)
    fasttrips.FastTripsLogger.info("Wrote %s" % output_file)
