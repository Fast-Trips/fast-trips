import os, sys
import pandas as pd

USAGE = r"""

    python create_tableau_route_map.py input_network_dir

    Reads from input_network_dir
      shapes.txt         (gtfs file)
      walk_access_ft.txt (gtfs-plus file)
      node_coords.csv    (columns N,X,Y)

    Todo if this turns out to be useful:
    Read drive_access_ft.txt and drive_access_points_ft.txt and add those links too.

    Outputs input_network_dir/tableau_map_links.csv with columns the same as shapes.txt
    But with the access and egress links added

      shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled

"""

if __name__ == "__main__":

    print sys.argv

    if len(sys.argv) != 2:
        print USAGE
        sys.exit(2)

    INPUT_DIR = sys.argv[1]

    # read shapes file -- we'll start with this
    shapes_file = os.path.join(INPUT_DIR, "shapes.txt")
    shapes_df = pd.read_csv(shapes_file)
    print "Read %d lines from %s" % (len(shapes_df), shapes_file)
    # print shapes_df.head()

    # read node coordinates, including tazs
    node_coords_file = os.path.join(INPUT_DIR, "node_coords.csv")
    node_coords_df = pd.read_csv(node_coords_file)
    print "Read %d lines from %s" % (len(node_coords_df), node_coords_file)

    # rename column to match shapes
    node_coords_df.rename(columns={"X":"shape_pt_lon",
                                   "Y":"shape_pt_lat"}, inplace=True)

    # todo: I don't think we need this but that's because sfcta shape_ids are route_ids, so it may help in the future
    # read trips file
    # trips_file = os.path.join(INPUT_DIR, "trips.txt")
    # trips_df = pd.read_csv(trips_file)
    # print trips_df.head()

    # read the walk_access_ft file
    walk_access_file = os.path.join(INPUT_DIR, "walk_access_ft.txt")
    walk_access_df = pd.read_csv(walk_access_file)
    # we only want taz, stop_id, dist
    walk_access_df = walk_access_df[["taz","stop_id","dist"]]
    print "Read %d lines from %s" % (len(walk_access_df), walk_access_file)

    # make the shape id and melt it for access links
    walk_access_df["shape_id"] = "access " + walk_access_df["taz"].map(str) + " " + walk_access_df["stop_id"].map(str)
    # print "walk_access: length %d; head:" % len(walk_access_df)
    # print walk_access_df.head()
    access_df = pd.melt(walk_access_df, id_vars=["shape_id","dist"], value_vars=["taz","stop_id"]).sort_values(by=["shape_id","variable"], ascending=[True,False]).reset_index(drop=True)
    access_df.replace(to_replace={"taz":0,"stop_id":1}, inplace=True)

    # re-make the shape id and melt it for egress links
    walk_access_df["shape_id"] = "egress " + walk_access_df["stop_id"].map(str) + " " + walk_access_df["taz"].map(str)
    egress_df = pd.melt(walk_access_df, id_vars=["shape_id","dist"], value_vars=["taz","stop_id"]).sort_values(by=["shape_id","variable"], ascending=[True,True]).reset_index(drop=True)
    egress_df.replace(to_replace={"stop_id":0, "taz":1}, inplace=True)
    # print len(egress_df)
    # print egress_df.head()

    # put access and egress together
    access_egress_df = pd.concat([access_df, egress_df])
    # rename the columns
    access_egress_df.rename(columns={"dist":"shape_dist_traveled",
                                     "variable":"shape_pt_sequence",
                                     "value":"N"}, inplace=True)
    # print len(access_egress_df)
    # print access_egress_df.head()

    # join with node_coords to get node coordinates
    access_egress_df = pd.merge(left=access_egress_df, right=node_coords_df, on="N", how="left")
    # drop N now we're done with it
    access_egress_df.drop("N", axis=1, inplace=True)
    # print len(access_egress_df)
    # print access_egress_df.head()

    print "Out of %d (non-unique) nodes, %d failed to find coordinates" % (len(access_egress_df), pd.isnull(access_egress_df["shape_pt_lon"]).sum())

    # append to shapes and save
    shapes_df = pd.concat([shapes_df, access_egress_df])
    outfile = os.path.join(INPUT_DIR, "tableau_map_links.csv")
    shapes_df.to_csv(outfile, index=False, header=True)
    print "Wrote %d lines to %s" % (len(shapes_df), outfile)