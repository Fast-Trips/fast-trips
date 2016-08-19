import os, sys
import fasttrips
import pandas

USAGE = r"""

    python create_tableau_path_map.py input_network_dir input_demand_dir paths.csv

    Creates a tableau file to map paths by path description.

    """

if __name__ == "__main__":

    pandas.set_option('display.width',      1000)
    # pandas.set_option('display.height',   1000)
    pandas.set_option('display.max_rows',   1000)
    pandas.set_option('display.max_columns', 100)

    if len(sys.argv) != 3:
        print USAGE
        sys.exit(2)

    NETWORK_DIR = sys.argv[1]
    PATHS_DIR   = sys.argv[2]
    OUTPUT_DIR  = "create_tableau_path_map"

    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    # Use fasttrips to read the network
    ft = fasttrips.FastTrips(input_network_dir= NETWORK_DIR,
                             input_demand_dir = None,
                             output_dir       = OUTPUT_DIR)
    ft.read_input_files()

    # and the pathset files
    (pathset_paths_df, pathset_links_df) = fasttrips.Passenger.read_passenger_pathsets(PATHS_DIR)

    # split the pathset links into component bits
    veh_trips_df     = ft.trips.get_full_trips()
    pathset_links_df = fasttrips.PathSet.split_transit_links(pathset_links_df, veh_trips_df, ft.stops)

    # just need the taz coords
    taz_coords_file = os.path.join(NETWORK_DIR, "taz_coords.txt")
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
    pathset_links_df = pathset_links_df[["person_id","person_trip_id","trip_list_id_num","iteration","simulation_iteration","pathnum",
                                         "linknum","linkmode","mode","route_id","trip_id",
                                         "A_id","A_seq","A_lat","A_lon",
                                         "B_id","B_seq","B_lat","B_lon"]]

    map_points_df = pathset_links_df[["person_id","person_trip_id","trip_list_id_num","iteration","simulation_iteration","pathnum",
                                      "linknum","linkmode","mode","route_id","trip_id",
                                      "A_id","A_seq","A_lat","A_lon"]].copy()

    # Each link will be it's own map line.  A split link is a single map line but with a bunch of points.
    # Access, egresss and transit map lines will just be two points: A and B
    # So the line index is "person_id","person_trip_id","trip_list_id_num","iteration","simulation_iteration","pathnum","linknum"

    # add point id starting with 1 for A; the B point IS is just that plus one
    map_points_df["point_id"] = map_points_df.groupby(["person_id","person_trip_id","trip_list_id_num","iteration","simulation_iteration","pathnum","linknum"]).cumcount() + 1
    # rename A_id, A_seq, A_lat, A_lon
    map_points_df.rename(columns={"A_id" :"stop_or_taz_id",
                                  "A_seq":"stop_sequence",
                                  "A_lat":"latitude",
                                  "A_lon":"longitude"}, inplace=True)

    fasttrips.FastTripsLogger.debug("map_points_df head=\n%s\ntail=\n%s" % (str(map_points_df.head(30)), str(map_points_df.tail(30))))

    # group for the last one
    pathset_links_df["point_id"]=1  # we need this set to *something* agg
    last_point_df = pathset_links_df.groupby(["person_id","person_trip_id","trip_list_id_num","iteration","simulation_iteration","pathnum","linknum"]).agg(
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
    map_points_df.sort_values(by=["person_id","person_trip_id","trip_list_id_num","iteration","simulation_iteration","pathnum","linknum","point_id"], inplace=True)
    map_points_df.reset_index(drop=True, inplace=True)

    map_points_df[["pathnum","linknum","trip_list_id_num"]] = map_points_df[["pathnum","linknum","trip_list_id_num"]].astype(int)
    fasttrips.FastTripsLogger.debug("map_points_df head=\n%s\ntail=\n%s" % (str(map_points_df.head(100)), str(map_points_df.tail(100))))

    # write it
    output_file = os.path.join(PATHS_DIR, "pathset_map_points.csv")
    map_points_df.to_csv(output_file, sep=",", index=False)
    fasttrips.FastTripsLogger.info("Wrote %s" % output_file)
