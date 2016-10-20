import argparse, os, sys
import fasttrips
import pandas

USAGE = r"""

    Creates a tableau file with points for pathset paths.

    """

if __name__ == "__main__":

    pandas.set_option('display.width',      1000)
    pandas.set_option('display.max_rows',   1000)
    pandas.set_option('display.max_columns', 100)

    parser = argparse.ArgumentParser(description=USAGE)
    parser.add_argument('input_network_dir', type=str, nargs=1, help="Directory with input network")
    parser.add_argument('input_path_dir',    type=str, nargs=1, help="Directory with pathset_[links,paths].csv files")
    parser.add_argument('--description', dest='use_description', action='store_true', 
        help="Specify this to use path description as ID.  Otherwise will use standard pathset fields (person_id, person_trip_id, iteration, simulation_iteration, pathnum)")

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
    (pathset_paths_df, pathset_links_df) = fasttrips.Passenger.read_passenger_pathsets(args.input_path_dir[0], ft.stops)

    fasttrips.FastTripsLogger.debug("Read passenger pathsets. head=\n%s" % str(pathset_links_df.head(30)))
    # add stop lats and lons if they're not there
    if "A_lat" not in pathset_links_df.columns.values:
      pathset_links_df = ft.stops.add_stop_lat_lon(pathset_links_df,
                                                   id_colname="A_id",
                                                   new_lat_colname="A_lat",
                                                   new_lon_colname="A_lon")
      pathset_links_df = ft.stops.add_stop_lat_lon(pathset_links_df,
                                                   id_colname="B_id",
                                                   new_lat_colname="B_lat",
                                                   new_lon_colname="B_lon")
      # not valid for access A_id and egress B_id
      pathset_links_df.loc[pathset_links_df["linkmode"]=="access", "A_lat"] = None
      pathset_links_df.loc[pathset_links_df["linkmode"]=="access", "A_lon"] = None
      pathset_links_df.loc[pathset_links_df["linkmode"]=="egress", "B_lat"] = None
      pathset_links_df.loc[pathset_links_df["linkmode"]=="egress", "B_lon"] = None
      fasttrips.FastTripsLogger.debug("Added stop lots and lons. head=\n%s" % str(pathset_links_df.head(30)))

    # split the pathset links into component bits
    veh_trips_df     = ft.trips.get_full_trips()
    pathset_links_df = fasttrips.PathSet.split_transit_links(pathset_links_df, veh_trips_df, ft.stops)

    # just need the taz coords
    taz_coords_file = os.path.join(args.input_network_dir[0], "taz_coords.txt")
    taz_coords_df = pandas.read_csv(taz_coords_file, dtype={"taz":object})
    fasttrips.FastTripsLogger.debug("taz_coords_df=\n%s" % str(taz_coords_df))

    # join to links
    pathset_links_df = pandas.merge(left    =pathset_links_df,
                                    left_on ="A_id",
                                    right   =taz_coords_df,
                                    right_on="taz",
                                    how     ="left")
    # get lat, lon for access links
    pathset_links_df.loc[pathset_links_df["linkmode"]=="access", "A_lat"] = pathset_links_df["lat"]
    pathset_links_df.loc[pathset_links_df["linkmode"]=="access", "A_lon"] = pathset_links_df["lon"]
    pathset_links_df.drop(["taz","lat","lon"], axis=1, inplace=True)

    pathset_links_df = pandas.merge(left    =pathset_links_df,
                                    left_on ="B_id",
                                    right   =taz_coords_df,
                                    right_on="taz",
                                    how     ="left")
    # get lat, lon for egress links
    pathset_links_df.loc[pathset_links_df["linkmode"]=="egress", "B_lat"] = pathset_links_df["lat"]
    pathset_links_df.loc[pathset_links_df["linkmode"]=="egress", "B_lon"] = pathset_links_df["lon"]
    pathset_links_df.drop(["taz","lat","lon"], axis=1, inplace=True)

    missing_lat_lon = pathset_links_df.loc[pandas.isnull(pathset_links_df["A_lat"])|pandas.isnull(pathset_links_df["B_lat"])]
    if len(missing_lat_lon) > 0:
        fasttrips.FastTripsLogger.info("Missing lat/lons for\n%s" % str(missing_lat_lon))

    # select out just the fields we want for the map
    pathset_links_df = pathset_links_df[["person_id","person_trip_id","iteration","simulation_iteration","pathnum",
                                         "linknum","linkmode","mode","route_id","trip_id",
                                         "A_id","A_seq","A_lat","A_lon",
                                         "B_id","B_seq","B_lat","B_lon"]]

    map_points_df = pathset_links_df[["person_id","person_trip_id","iteration","simulation_iteration","pathnum",
                                      "linknum","linkmode","mode","route_id","trip_id",
                                      "A_id","A_seq","A_lat","A_lon"]].copy()

    pathset_paths_df = pathset_paths_df[["person_id","person_trip_id","iteration","simulation_iteration","pathnum",
                                         "description"]]
    pathset_paths_df[["pathnum"]] = pathset_paths_df[["pathnum"]].astype(int)

    # Each link will be it's own map line.  A split link is a single map line but with a bunch of points.
    # Access, egresss and transit map lines will just be two points: A and B
    # So the line index is "person_id","person_trip_id","iteration","simulation_iteration","pathnum","linknum"

    # add point id starting with 1 for A; the B point IS is just that plus one
    map_points_df["point_id"] = map_points_df.groupby(["person_id","person_trip_id","iteration","simulation_iteration","pathnum","linknum"]).cumcount() + 1
    # rename A_id, A_seq, A_lat, A_lon
    map_points_df.rename(columns={"A_id" :"stop_or_taz_id",
                                  "A_seq":"stop_sequence",
                                  "A_lat":"latitude",
                                  "A_lon":"longitude"}, inplace=True)

    fasttrips.FastTripsLogger.debug("map_points_df head=\n%s\ntail=\n%s" % (str(map_points_df.head(30)), str(map_points_df.tail(30))))

    # group for the last one
    pathset_links_df["point_id"]=1  # we need this set to *something* agg
    last_point_df = pathset_links_df.groupby(["person_id","person_trip_id","iteration","simulation_iteration","pathnum","linknum"]).agg(
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
    map_points_df.sort_values(by=["person_id","person_trip_id","iteration","simulation_iteration","pathnum","linknum","point_id"], inplace=True)
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
        map_points_df = pandas.merge(left    =pathset_paths_df,
                                     left_on =["person_id","person_trip_id","iteration","simulation_iteration","pathnum"],
                                     right   =map_points_df,
                                     right_on=["person_id","person_trip_id","iteration","simulation_iteration","pathnum"],
                                     how     ="inner")
        fasttrips.FastTripsLogger.info("Have %d map points" % len(map_points_df))

        # drop the non-descript columns
        map_points_df.drop(["person_id","person_trip_id","iteration","simulation_iteration","pathnum"], axis=1, inplace=True)

    # write it
    output_file = os.path.join(args.input_path_dir[0], "pathset_map_points.csv")
    map_points_df.to_csv(output_file, sep=",", index=False)
    fasttrips.FastTripsLogger.info("Wrote %s" % output_file)
